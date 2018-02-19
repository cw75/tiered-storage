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
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"
#include "common.h"

using namespace std;

/*string handle_request(
    string input,
    SocketCache& pushers,
    vector<string>& proxy_address,
    unordered_map<string, unordered_set<string>>& key_address_cache,
    unsigned& seed,
    user_thread_t& ut,
    zmq::socket_t& response_puller,
    zmq::socket_t& key_address_puller,
    string& ip,
    unsigned& thread_id,
    unsigned& rid) {
  vector<string> v;
  split(input, ' ', v);

  if (v.size() == 0) {
    return "Empty request.\n";
  } else if (v[0] != "GET" && v[0] != "PUT") { 
    return "Invalid request: " + v[0] + ".\n";
  } else {
    string key = v[1];
    communication::Request req;
    req.set_type(v[0]);
    req.set_respond_address(ut.get_request_pulling_connect_addr());
    if (v[0] == "GET") {
      prepare_get_tuple(req, key);
    } else {
      prepare_put_tuple(req, key, v[2], 0);
    }
    // get worker address
    string worker_address;
    if (key_address_cache.find(key) == key_address_cache.end()) {
      // query the proxy and update the cache
      string target_proxy_address = get_random_proxy_thread(proxy_address, seed).get_key_address_connect_addr();
      bool succeed;
      auto addresses = get_address_from_proxy(ut, key, pushers[target_proxy_address], key_address_puller, succeed, ip, thread_id, rid);
      if (succeed) {
        for (auto it = addresses.begin(); it != addresses.end(); it++) {
          key_address_cache[key].insert(*it);
        }
        worker_address = addresses[rand_r(&seed) % addresses.size()];
      } else {
        return "request timed out when querying proxy\n";
      }
    } else {
      worker_address = *(next(begin(key_address_cache[key]), rand_r(&seed) % key_address_cache[key].size()));
    }
    bool succeed;
    auto res = send_request<communication::Request, communication::Response>(req, pushers[worker_address], response_puller, succeed);
    if (succeed) {
      // initialize the respond string
      if (v[0] == "GET") {
        if (res.tuple(0).err_number() == 0) {
          return "value of key " + res.tuple(0).key() + " is " + res.tuple(0).value() + "\n";
        } else if (res.tuple(0).err_number() == 1) {
          return "key " + res.tuple(0).key() + " does not exist\n";
        } else {
          // update cache and retry
          cerr << "cache invalidation\n";
          key_address_cache.erase(key);
          return handle_request(input, pushers, proxy_address, key_address_cache, seed, ut, response_puller, key_address_puller);
        }
      } else {
        if (res.tuple(0).err_number() == 0) {
          return "successfully put key " + res.tuple(0).key() + "\n";
        } else {
          // update cache and retry
          cerr << "cache invalidation\n";
          key_address_cache.erase(key);
          return handle_request(input, pushers, proxy_address, key_address_cache, seed, ut, response_puller, key_address_puller);
        }
      }
    } else {
      return "request timed out when querying worker\n";
    }
  }
}*/

int main(int argc, char* argv[]) {

  bool batch;
  if (argc == 1) {
    batch = false;
  } else if (argc == 2) {
    batch = true;
  } else {
    cerr << "invalid argument" << endl;
    return 1;
  }

  /*string ip = get_ip("user");

  // read in the proxy addresses
  vector<string> proxy_address;

  // mapping from key to a set of worker addresses
  unordered_map<string, unordered_set<string>> key_address_cache;

  unsigned seed = time(NULL);

  user_thread_t ut = user_thread_t(ip, 0);

  // read proxy address from the file
  string ip_line;
  ifstream address;
  address.open("conf/user/proxy_address.txt");
  while (getline(address, ip_line)) {
    proxy_address.push_back(ip_line);
  }
  address.close();

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  int timeout = 5000;
  // responsible for pulling response
  zmq::socket_t response_puller(context, ZMQ_PULL);
  response_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  response_puller.bind(ut.get_request_pulling_bind_addr());
  // responsible for receiving depart done notice
  zmq::socket_t key_address_puller(context, ZMQ_PULL);
  key_address_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  key_address_puller.bind(ut.get_key_address_bind_addr());

  if (!batch) {
    string input;
    while (true) {
      cout << "kvs> ";
      getline(cin, input);
      cout << handle_request(input, pushers, proxy_address, key_address_cache, seed, ut, response_puller, key_address_puller);
    }
  } else {
    // read in the request
    string request;
    ifstream request_reader;
    request_reader.open(argv[1]);
    while (getline(request_reader, request)) {
      cout << handle_request(request, pushers, proxy_address, key_address_cache, seed, ut, response_puller, key_address_puller);
    }
  }*/
}
