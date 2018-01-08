#include <zmq.hpp>
#include <string>
#include <stdlib.h>
#include <sstream>
#include <fstream>
#include <vector>
#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include <memory>
#include "rc_kv_store.h"
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"
#include "consistent_hash_map.hpp"
#include "common.h"

using namespace std;
using address_t = string;

struct replication_factor_request {
  replication_factor_request() : key_(""), global_memory_replication_(1), global_ebs_replication_(2) {}
  replication_factor_request(string k, int gmr, int ger)
    : key_(k), global_memory_replication_(gmr), global_ebs_replication_(ger) {}
  string key_;
  int global_memory_replication_;
  int global_ebs_replication_;
};

void change_replication_factor(vector<replication_factor_request> requests,
    global_hash_t& global_memory_hash_ring,
    global_hash_t& global_ebs_hash_ring,
    vector<address_t>& proxy_address,
    unordered_map<string, key_info>& placement,
    SocketCache& pushers) {
  // used to keep track of the original replication factors for the requested keys
  unordered_map<string, pair<int, int>> orig_placement_info;

  // form the placement request map
  unordered_map<address_t, communication::Replication_Factor_Request> replication_factor_request_map;

  for (auto key_iter = requests.begin(); key_iter != requests.end(); key_iter++) {
    string key = key_iter->key_;
    orig_placement_info[key] = pair<int, int>(placement[key].global_memory_replication_, placement[key].global_ebs_replication_);
    // update the placement map
    placement[key].global_memory_replication_ = key_iter->global_memory_replication_;
    placement[key].global_ebs_replication_ = key_iter->global_ebs_replication_;
    // for each tier, take the max between the key's original rep factor and current rep factor
    int memory_rep = max(placement[key].global_memory_replication_, orig_placement_info[key].first);
    int ebs_rep = max(placement[key].global_ebs_replication_, orig_placement_info[key].second);

    // form placement requests for memory tier nodes
    auto server_iter = global_memory_hash_ring.find(key);
    if (server_iter != global_memory_hash_ring.end()) {
      for (int i = 0; i < memory_rep; i++) {
        communication::Replication_Factor_Request_Tuple* tp = replication_factor_request_map[server_iter->second.replication_factor_connect_addr_].add_tuple();
        tp->set_key(key);
        tp->set_global_memory_replication(placement[key].global_memory_replication_);
        tp->set_global_ebs_replication(placement[key].global_ebs_replication_);

        if (++server_iter == global_memory_hash_ring.end()) {
          server_iter = global_memory_hash_ring.begin();
        }
      }
    }

    // form placement requests for ebs tier nodes
    server_iter = global_ebs_hash_ring.find(key);
    if (server_iter != global_ebs_hash_ring.end()) {
      for (int i = 0; i < ebs_rep; i++) {
        communication::Replication_Factor_Request_Tuple* tp = replication_factor_request_map[server_iter->second.replication_factor_connect_addr_].add_tuple();
        tp->set_key(key);
        tp->set_global_memory_replication(placement[key].global_memory_replication_);
        tp->set_global_ebs_replication(placement[key].global_ebs_replication_);

        if (++server_iter == global_ebs_hash_ring.end()) {
          server_iter = global_ebs_hash_ring.begin();
        }
      }
    }

    // form placement requests for proxy nodes
    for (auto proxy_iter = proxy_address.begin(); proxy_iter != proxy_address.end(); proxy_iter++) {
      communication::Replication_Factor_Request_Tuple* tp = replication_factor_request_map[proxy_node_t(*proxy_iter).replication_factor_connect_addr_].add_tuple();
      tp->set_key(key);
      tp->set_global_memory_replication(placement[key].global_memory_replication_);
      tp->set_global_ebs_replication(placement[key].global_ebs_replication_);
    }
  }

  // send placement info update to all relevant server nodes
  for (auto it = replication_factor_request_map.begin(); it != replication_factor_request_map.end(); it++) {
    string data;
    it->second.SerializeToString(&data);
    zmq_util::send_string(data, &pushers[it->first]);
  }

  // update the replication factor in storage servers
  communication::Request req;
  req.set_type("PUT");
  req.set_metadata(true);

  for (auto key_iter = requests.begin(); key_iter != requests.end(); key_iter++) {
    communication::Request_Tuple* tp = req.add_tuple();
    tp->set_key(key_iter->key_ + "_replication");
    tp->set_value(to_string(key_iter->global_memory_replication_) + ":" + to_string(key_iter->global_ebs_replication_));
  }
  string serialized_req;
  req.SerializeToString(&serialized_req);
  // just pick the first proxy to contact for now;
  // this should eventually be round-robin / random
  string proxy_ip = *(proxy_address.begin());
  // randomly choose a proxy thread to connect
  int tid = 1 + rand() % PROXY_THREAD_NUM;
  zmq_util::send_string(serialized_req, &pushers[proxy_worker_thread_t(proxy_ip, tid).metadata_connect_addr_]);
}

// TODO: instead of cout or cerr, everything should be written to a log file.
int main(int argc, char* argv[]) {

  auto logger = spdlog::basic_logger_mt("basic_logger", "log.txt", true);
  logger->flush_on(spdlog::level::info); 

  if (argc != 1) {
    cerr << "usage:" << argv[0] << endl;
    return 1;
  }

  global_hash_t global_memory_hash_ring;
  global_hash_t global_ebs_hash_ring;

  // keep track of the keys' replication info
  unordered_map<string, key_info> placement;

  // keep track of the keys' hotness
  unordered_map<string, unordered_map<address_t, size_t>> key_access_frequency;

  // keep track of memory tier storage consumption
  unordered_map<address_t, unordered_map<string, size_t>> memory_tier_storage;

  // keep track of ebs tier storage consumption
  unordered_map<address_t, unordered_map<string, size_t>> ebs_tier_storage;
 
  // read in the initial server addresses and build the hash ring
  string ip_line;
  ifstream address;

  // read existing memory servers and populate the memory hash ring
  address.open("conf/monitoring/existing_memory_servers.txt");

  while (getline(address, ip_line)) {
    logger->info("{}", ip_line);
    //cerr << ip_line << "\n";
    global_memory_hash_ring.insert(master_node_t(ip_line, "M"));
  }
  address.close();

  // read existing ebs servers and populate the ebs hash ring
  address.open("conf/monitoring/existing_ebs_servers.txt");

  while (getline(address, ip_line)) {
    logger->info("{}", ip_line);
    //cerr << ip_line << "\n";
    global_ebs_hash_ring.insert(master_node_t(ip_line, "E"));
  }
  address.close();

  // read address of proxies from conf file
  vector<address_t> proxy_address;

  address.open("conf/monitoring/proxy_address.txt");
  while (getline(address, ip_line)) {
    proxy_address.push_back(ip_line);
  }
  address.close();

  // read address of management node from conf file
  address_t management_address;

  address.open("conf/monitoring/management_ip.txt");
  getline(address, ip_line);
  management_address = ip_line;
  address.close();


  zmq::context_t context(1);

  SocketCache pushers(&context, ZMQ_PUSH);
  SocketCache requesters(&context, ZMQ_REQ);

  // responsible for both node join and departure
  zmq::socket_t join_puller(context, ZMQ_PULL);
  join_puller.bind(NOTIFY_BIND_ADDR);

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0 },
  };

  auto hotness_start = std::chrono::system_clock::now();
  auto hotness_end = std::chrono::system_clock::now();

  auto storage_start = std::chrono::system_clock::now();
  auto storage_end = std::chrono::system_clock::now();

  bool adding_memory_node = false;
  bool adding_ebs_node = false;

  while (true) {
    // listen for ZMQ events
    zmq_util::poll(0, &pollitems);

    // handle a join or depart event coming from the server side
    if (pollitems[0].revents & ZMQ_POLLIN) {
      vector<string> v;
      split(zmq_util::recv_string(&join_puller), ':', v);
      if (v[0] == "join") {
        logger->info("received join");
        //cerr << "received join\n";
        // update hash ring
        if (v[1] == "M") {
          global_memory_hash_ring.insert(master_node_t(v[2], "M"));
          adding_memory_node = false;
        } else if (v[1] == "E") {
          global_ebs_hash_ring.insert(master_node_t(v[2], "E"));
          adding_ebs_node = false;
        } else if (v[1] == "P") {
          proxy_address.push_back(v[2]);
        } else {
          logger->info("Invalid Tier info");
          //cerr << "Invalid Tier info\n";
        }
        logger->info("memory hash ring size is {}", to_string(global_memory_hash_ring.size()));
        logger->info("ebs hash ring size is {}", to_string(global_ebs_hash_ring.size()));
        //cerr << "memory hash ring size is " + to_string(global_memory_hash_ring.size()) + "\n";
        //cerr << "ebs hash ring size is " + to_string(global_ebs_hash_ring.size()) + "\n";
      } else if (v[0] == "depart") {
        logger->info("received depart");
        //cerr << "received depart\n";
        // update hash ring
        if (v[1] == "M") {
          global_memory_hash_ring.erase(master_node_t(v[2], "M"));
          memory_tier_storage.erase(v[2]);
        } else if (v[1] == "E") {
          global_ebs_hash_ring.erase(master_node_t(v[2], "E"));
          ebs_tier_storage.erase(v[2]);
        } else {
          logger->info("Invalid Tier info");
          //cerr << "Invalid Tier info\n";
        }
        logger->info("memory hash ring size is {}", to_string(global_memory_hash_ring.size()));
        logger->info("ebs hash ring size is {}", to_string(global_ebs_hash_ring.size()));
        //cerr << "memory hash ring size is " + to_string(global_memory_hash_ring.size()) + "\n";
        //cerr << "ebs hash ring size is " + to_string(global_ebs_hash_ring.size()) + "\n";
      }
    }

    hotness_end = std::chrono::system_clock::now();

    if (chrono::duration_cast<std::chrono::seconds>(hotness_end-hotness_start).count() >= HOTNESS_MONITORING_PERIOD) {
      // fetch key hotness data from the storage tier
      communication::Request req;
      req.set_type("GET");
      req.set_metadata(true);

      for (auto it = placement.begin(); it != placement.end(); it++) {
        for (auto iter = proxy_address.begin(); iter != proxy_address.end(); iter++) {
          communication::Request_Tuple* tp = req.add_tuple();
          tp->set_key(it->first + "_" + *iter + "_hotness");
        }
      }

      string serialized_req;
      req.SerializeToString(&serialized_req);
      // just pick the first proxy to contact for now;
      // this should eventually be round-robin / random
      string proxy_ip = *(proxy_address.begin());
      // randomly choose a proxy thread to connect
      int tid = 1 + rand() % PROXY_THREAD_NUM;
      zmq_util::send_string(serialized_req, &requesters[proxy_worker_thread_t(proxy_ip, tid).request_connect_addr_]);
      string serialized_resp = zmq_util::recv_string(&requesters[proxy_worker_thread_t(proxy_ip, tid).request_connect_addr_]);

      communication::Response resp;
      resp.ParseFromString(serialized_resp);

      for (int i = 0; i < resp.tuple_size(); i++) {
        if (resp.tuple(i).succeed()) {
          vector<string> tokens;
          split(resp.tuple(i).key(), '_', tokens);
          string key = tokens[0];
          string node_ip = tokens[1];
          size_t access = stoi(resp.tuple(i).value());
          key_access_frequency[key][node_ip] = access;
        }
      }

      hotness_start = std::chrono::system_clock::now();
    }

    storage_end = std::chrono::system_clock::now();

    if (chrono::duration_cast<std::chrono::seconds>(storage_end-storage_start).count() >= STORAGE_CONSUMPTION_REPORT_THRESHOLD) {
      // fetch storage consumption data from the storage tier
      communication::Request req;
      req.set_type("GET");
      req.set_metadata(true);

      for (auto it = global_memory_hash_ring.begin(); it != global_memory_hash_ring.end(); it++) {
        for (int tid = 1; tid <= MEMORY_THREAD_NUM; tid++) {
          communication::Request_Tuple* tp = req.add_tuple();
          tp->set_key(it->second.ip_ + "_M_" + to_string(tid) + "_storage");
        }
      }

      for (auto it = global_ebs_hash_ring.begin(); it != global_ebs_hash_ring.end(); it++) {
        for (int tid = 1; tid <= EBS_THREAD_NUM; tid++) {
          communication::Request_Tuple* tp = req.add_tuple();
          tp->set_key(it->second.ip_ + "_E_" + to_string(tid) + "_storage");
        }
      }

      string serialized_req;
      req.SerializeToString(&serialized_req);
      // just pick the first proxy to contact for now;
      // this should eventually be round-robin / random
      string proxy_ip = *(proxy_address.begin());
      // randomly choose a proxy thread to connect
      int tid = 1 + rand() % PROXY_THREAD_NUM;
      zmq_util::send_string(serialized_req, &requesters[proxy_worker_thread_t(proxy_ip, tid).request_connect_addr_]);
      string serialized_resp = zmq_util::recv_string(&requesters[proxy_worker_thread_t(proxy_ip, tid).request_connect_addr_]);

      communication::Response resp;
      resp.ParseFromString(serialized_resp);

      for (int i = 0; i < resp.tuple_size(); i++) {
        if (resp.tuple(i).succeed()) {
          vector<string> tokens;
          split(resp.tuple(i).key(), '_', tokens);
          string ip = tokens[0];
          string type = tokens[1];

          if (type == "M") {
            memory_tier_storage[tokens[0]][tokens[2]] = stoi(resp.tuple(i).value());
          } else {
            ebs_tier_storage[tokens[0]][tokens[2]] = stoi(resp.tuple(i).value());
          }
        }
      }

      size_t total_memory_consumption = 0;
      size_t total_ebs_consumption = 0;
      int memory_node_count = 0;
      int ebs_volume_count = 0;

      for (auto it1 = memory_tier_storage.begin(); it1 != memory_tier_storage.end(); it1++) {
        for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
          total_memory_consumption += it2->second;
        }
        memory_node_count += 1;
      }

      for (auto it1 = ebs_tier_storage.begin(); it1 != ebs_tier_storage.end(); it1++) {
        for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
          total_ebs_consumption += it2->second;
          ebs_volume_count += 1;
        }
      }

      if ((double)total_memory_consumption / (double)memory_node_count >= 10 && !adding_memory_node) {
        logger->info("trigger add memory node");
        //cerr << "trigger add memory node\n";
        string shell_command = "curl -X POST https://" + management_address + "/memory";
        system(shell_command.c_str());
        adding_memory_node = true;
      }

      if ((double)total_ebs_consumption / (double)ebs_volume_count >= 100 && !adding_ebs_node) {
        logger->info("trigger add ebs node");
        //cerr << "trigger add ebs node\n";
        string shell_command = "curl -X POST https://" + management_address + "/ebs";
        system(shell_command.c_str());
        adding_ebs_node = true;
      }

      storage_start = std::chrono::system_clock::now();
    }

    // TODO: Add policy that triggers node join/removal and key replication factor change

  }
}
