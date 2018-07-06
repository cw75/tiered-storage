#include <zmq.hpp>
#include <string>
#include <stdlib.h>
#include <sstream>
#include <fstream>
#include <vector>
#include <iostream>
#include <unistd.h>
#include <memory>
#include <unordered_set>
#include "spdlog/spdlog.h"
#include "communication.pb.h"
#include "zmq/socket_cache.hpp"
#include "zmq/zmq_util.hpp"
#include "common.hpp"
#include "threads.hpp"
#include "requests.hpp"
#include "hashers.hpp"
#include "hash_ring.hpp"
#include "yaml-cpp/yaml.h"

using namespace std;

unsigned ROUTING_THREAD_NUM;
unsigned DEFAULT_LOCAL_REPLICATION;

void handle_request(
    string request_line,
    SocketCache& pushers,
    vector<string>& routing_address,
    unordered_map<string, unordered_set<string>>& key_address_cache,
    unsigned& seed,
    shared_ptr<spdlog::logger> logger,
    UserThread& ut,
    zmq::socket_t& response_puller,
    zmq::socket_t& key_address_puller,
    string& ip,
    unsigned& thread_id,
    unsigned& rid,
    unsigned& trial) {

  vector<string> v;
  split(request_line, ' ', v);
  string key, value;

  if (!((v.size() == 2 && v[0] == "GET") || (v.size() == 3 && v[0] == "PUT"))) {
    cout << "Usage: GET <key> | PUT <key> <value>" << endl;
    return;
  } else {
    if (v[0] == "GET") {
      key = v[1];
      value = "";
    } else {
      key = v[1];
      value = v[2];
    }
  }

  if (trial > 5) {
    logger->info("Trial #{} for request for key {}.", trial, key);
    logger->info("Waiting 5 seconds.");
    chrono::seconds dura(5);
    this_thread::sleep_for(dura);
    logger->info("Waited 5s.");
  }

  // get worker address
  string worker_address;
  if (key_address_cache.find(key) == key_address_cache.end()) {
    // query the routing and update the cache
    string target_routing_address = get_random_routing_thread(routing_address, seed, ROUTING_THREAD_NUM).get_key_address_connect_addr();
    bool succeed;
    vector<string> addresses = get_address_from_routing(ut, key, pushers[target_routing_address], key_address_puller, succeed, ip, thread_id, rid);

    if (succeed) {
      for (auto it = addresses.begin(); it != addresses.end(); it++) {
        key_address_cache[key].insert(*it);
      }
      worker_address = addresses[rand_r(&seed) % addresses.size()];
    } else {
      logger->error("Request timed out when querying routing. This should never happen!");
      return;
    }
  } else {
    if (key_address_cache[key].size() == 0) {
      logger->error("Address cache for key " + key + " has size 0.");
      return;
    }

    worker_address = *(next(begin(key_address_cache[key]), rand_r(&seed) % key_address_cache[key].size()));
  }

  communication::Request req;
  req.set_respond_address(ut.get_request_pulling_connect_addr());

  string req_id = ip + ":" + to_string(thread_id) + "_" + to_string(rid);
  req.set_request_id(req_id);
  rid += 1;

  if (value == "") {
    // get request
    req.set_type("GET");
    communication::Request_Tuple* tp = req.add_tuple();
    tp->set_key(key);
    tp->set_num_address(key_address_cache[key].size());
  } else {
    // put request
    req.set_type("PUT");
    communication::Request_Tuple* tp = req.add_tuple();
    tp->set_key(key);
    tp->set_value(value);
    tp->set_timestamp(0);
    tp->set_num_address(key_address_cache[key].size());
  }

  bool succeed;
  auto res = send_request<communication::Request, communication::Response>(req, pushers[worker_address], response_puller, succeed);

  if (succeed) {
    // initialize the respond string
    if (res.tuple(0).err_number() == 2) {
      trial += 1;
      if (trial > 5) {
        for (int i = 0; i < res.tuple(0).addresses_size(); i++) {
          logger->info("Server's return address for key {} is {}.", key, res.tuple(0).addresses(i));
        }
        for (auto it = key_address_cache[key].begin(); it != key_address_cache[key].end(); it++) {
          logger->info("My cached address for key {} is {}", key, *it);
        }
      }

      // update cache and retry
      key_address_cache.erase(key);
      handle_request(request_line, pushers, routing_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
    } else {
      // succeeded
      if (res.tuple(0).has_invalidate() && res.tuple(0).invalidate()) {
        // update cache
        key_address_cache.erase(key);
      }
      if (value == "" && res.tuple(0).err_number() == 0) {
        cout << "value of key " + res.tuple(0).key() + " is " + res.tuple(0).value() + "\n";
      } else if (value == "" && res.tuple(0).err_number() == 1) {
        cout << "key " + res.tuple(0).key() + " does not exist\n";
      } else if (value != "") {
        cout << "successfully put key " + res.tuple(0).key() + "\n";
      }
    }
  } else {
    logger->info("Request timed out when querying worker: clearing cache due to possible node membership changes.");
    // likely the node has departed. We clear the entries relavant to the worker_address
    vector<string> tokens;
    split(worker_address, ':', tokens);
    string signature = tokens[1];
    unordered_set<string> remove_set;

    for (auto it = key_address_cache.begin(); it != key_address_cache.end(); it++) {
      for (auto iter = it->second.begin(); iter != it->second.end(); iter++) {
        vector<string> v;
        split(*iter, ':', v);
        if (v[1] == signature) {
          remove_set.insert(it->first);
        }
      }
    }

    for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
      key_address_cache.erase(*it);
    }

    trial += 1;
    handle_request(request_line, pushers, routing_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
  }
}

void run(unsigned thread_id, string filename) {
  string log_file = "log_user.txt";
  string logger_name = "user_log";
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  // read the YAML conf
  YAML::Node conf = YAML::LoadFile("conf/config.yml")["user"];
  string ip = conf["ip"].as<string>();

  hash<string> hasher;
  unsigned seed = time(NULL);
  seed += hasher(ip);
  seed += thread_id;
  logger->info("Random seed is {}.", seed);


  // mapping from key to a set of worker addresses
  unordered_map<string, unordered_set<string>> key_address_cache;

  UserThread ut = UserThread(ip, thread_id);

  YAML::Node routing = conf["routing"];
  vector<string> routing_address;

  for (YAML::const_iterator it = routing.begin(); it != routing.end(); ++it) {
    routing_address.push_back(it->as<string>());
  }

  int timeout = 10000;
  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);


  // responsible for pulling response
  zmq::socket_t response_puller(context, ZMQ_PULL);
  response_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  response_puller.bind(ut.get_request_pulling_bind_addr());

  // responsible for receiving key address responses
  zmq::socket_t key_address_puller(context, ZMQ_PULL);
  key_address_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  key_address_puller.bind(ut.get_key_address_bind_addr());

  unsigned rid = 0;

  string input;
  unsigned trial = 1;
  if (filename == "") {
    while (true) {
      cout << "kvs> ";

      getline(cin, input);
      handle_request(input, pushers, routing_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
    }
  } else {
    ifstream infile(filename);

    while(getline(infile, input)) {
      handle_request(input, pushers, routing_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
    }
  }
}

int main(int argc, char* argv[]) {
  if (argc > 2) {
    cerr << "Usage: " << argv[0] << "<filename>" << endl;
    cerr << "Filename is optional. Omit the filename to run in interactive mode." << endl;
    return 1;
  }

  YAML::Node conf = YAML::LoadFile("conf/config.yml");
  ROUTING_THREAD_NUM = conf["threads"]["routing"].as<unsigned>();
  DEFAULT_LOCAL_REPLICATION = conf["replication"]["local"].as<unsigned>();

  if (argc == 1) {
    run(0, "");
  } else {
    run(0, argv[1]);
  }
}
