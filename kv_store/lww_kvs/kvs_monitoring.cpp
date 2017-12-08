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

#define DEFAULT_GLOBAL_MEMORY_REPLICATION 1
#define DEFAULT_GLOBAL_EBS_REPLICATION 0

struct replication_factor_request {
  replication_factor_request() : key_(""), global_memory_replication_(1), global_ebs_replication_(2) {}
  replication_factor_request(string k, int gmr, int ger)
    : key_(k), global_memory_replication_(gmr), global_ebs_replication_(ger) {}
  string key_;
  int global_memory_replication_;
  int global_ebs_replication_;
};

void change_replication_factor(vector<replication_factor_request> req,
    global_hash_t& global_memory_hash_ring,
    global_hash_t& global_ebs_hash_ring,
    vector<address_t>& proxy_address,
    unordered_map<string, key_info>& placement,
    SocketCache& pushers) {
  // used to keep track of the original replication factors for the requested keys
  unordered_map<string, pair<int, int>> orig_placement_info;

  // form the placement request map
  unordered_map<address_t, communication::Replication_Factor_Request> replication_factor_request_map;

  for (auto key_iter = req.begin(); key_iter != req.end(); key_iter++) {
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
}

// TODO: instead of cout or cerr, everything should be written to a log file.
int main(int argc, char* argv[]) {
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

  // keep track of memory tier worker thread occupancy
  unordered_map<master_node_t, float, node_hash> memory_tier_occupancy;

  // keep track of memory tier storage consumption
  unordered_map<master_node_t, size_t, node_hash> memory_tier_storage;

  // keep track of ebs tier storage consumption
  unordered_map<master_node_t, unordered_map<string, size_t>, node_hash> ebs_tier_storage;
 
  // read in the initial server addresses and build the hash ring
  string ip_line;
  ifstream address;

  // read existing memory servers and populate the memory hash ring
  address.open("conf/monitoring/existing_memory_servers.txt");

  while (getline(address, ip_line)) {
    cerr << ip_line << "\n";
    global_memory_hash_ring.insert(master_node_t(ip_line, "M"));
  }
  address.close();

  // read existing ebs servers and populate the ebs hash ring
  address.open("conf/monitoring/existing_ebs_servers.txt");

  while (getline(address, ip_line)) {
    cerr << ip_line << "\n";
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

  // responsible for both node join and departure
  zmq::socket_t join_puller(context, ZMQ_PULL);
  join_puller.bind(NOTIFY_BIND_ADDR);

  // responsible for responding key replication factor queries from the proxy worker threads
  zmq::socket_t replication_factor_query_responder(context, ZMQ_REP);
  replication_factor_query_responder.bind(REPLICATION_FACTOR_BIND_ADDR);

  // responsible for receiving storage consumption updates from server nodes
  zmq::socket_t storage_consumption_puller(context, ZMQ_PULL);
  storage_consumption_puller.bind(STORAGE_CONSUMPTION_BIND_ADDR);

  // responsible for receiving key hotness updates
  zmq::socket_t key_hotness_puller(context, ZMQ_PULL);
  key_hotness_puller.bind(KEY_HOTNESS_BIND_ADDR);

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(replication_factor_query_responder), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(storage_consumption_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(key_hotness_puller), 0, ZMQ_POLLIN, 0 }
  };

  while (true) {
    // listen for ZMQ events
    zmq_util::poll(0, &pollitems);

    // handle a join or depart event coming from the server side
    if (pollitems[0].revents & ZMQ_POLLIN) {
      vector<string> v;
      split(zmq_util::recv_string(&join_puller), ':', v);
      if (v[0] == "join") {
        cerr << "received join\n";
        // update hash ring
        if (v[1] == "M") {
          global_memory_hash_ring.insert(master_node_t(v[2], "M"));
        } else if (v[1] == "E") {
          global_ebs_hash_ring.insert(master_node_t(v[2], "E"));
        } else {
          cerr << "Invalid Tier info\n";
        }
        cerr << "memory hash ring size is " + to_string(global_memory_hash_ring.size()) + "\n";
        cerr << "ebs hash ring size is " + to_string(global_ebs_hash_ring.size()) + "\n";
      } else if (v[0] == "depart") {
        cerr << "received depart\n";
        // update hash ring
        if (v[1] == "M") {
          global_memory_hash_ring.erase(master_node_t(v[2], "M"));
          memory_tier_storage.erase(master_node_t(v[2], "M"));
        } else if (v[1] == "E") {
          global_ebs_hash_ring.erase(master_node_t(v[2], "E"));
          ebs_tier_storage.erase(master_node_t(v[2], "E"));
        } else {
          cerr << "Invalid Tier info\n";
        }
        cerr << "memory hash ring size is " + to_string(global_memory_hash_ring.size()) + "\n";
        cerr << "ebs hash ring size is " + to_string(global_ebs_hash_ring.size()) + "\n";
      }
    }

    if (pollitems[1].revents & ZMQ_POLLIN) {
      cerr << "received replication factor query\n";

      string key = zmq_util::recv_string(&replication_factor_query_responder);

      if (placement.find(key) == placement.end()) {
        placement[key] = key_info(DEFAULT_GLOBAL_MEMORY_REPLICATION, DEFAULT_GLOBAL_EBS_REPLICATION);
      }

      communication::Replication_Factor replication_factor;
      replication_factor.set_global_memory_replication(placement[key].global_memory_replication_);
      replication_factor.set_global_ebs_replication(placement[key].global_ebs_replication_);

      string response;

      replication_factor.SerializeToString(&response);
      zmq_util::send_string(response, &replication_factor_query_responder);
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      cerr << "received storage update\n";
      string storage_msg = zmq_util::recv_string(&storage_consumption_puller);
      communication::Storage_Update su;
      su.ParseFromString(storage_msg);
      if (su.node_type() == "M") {
        memory_tier_storage[master_node_t(su.node_ip(), "M")] = su.memory_storage();
        memory_tier_occupancy[master_node_t(su.node_ip(), "M")] = su.thread_occupancy();
      } else {
        ebs_tier_storage[master_node_t(su.node_ip(), "E")].clear();

        for (int i = 0; i < su.ebs_size(); i++) {
          ebs_tier_storage[master_node_t(su.node_ip(), "E")][su.ebs(i).id()] = su.ebs(i).storage();
        }
      }

      size_t total_memory_consumption = 0;
      size_t total_ebs_consumption = 0;
      int memory_node_count = 0;
      int ebs_volume_count = 0;

      for (auto it = memory_tier_storage.begin(); it != memory_tier_storage.end(); it++) {
        total_memory_consumption += it->second;
        memory_node_count += 1;
      }

      for (auto it1 = ebs_tier_storage.begin(); it1 != ebs_tier_storage.end(); it1++) {
        for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
          total_ebs_consumption += it2->second;
          ebs_volume_count += 1;
        }
      }

      if ((double)total_memory_consumption / (double)memory_node_count > 0) {
        cerr << "trigger add memory node\n";
        string shell_command = "curl -X POST https://" + management_address + "/memory";
        system(shell_command.c_str());
      }

      if ((double)total_ebs_consumption / (double)ebs_volume_count > 0) {
        cerr << "trigger add ebs node\n";
        string shell_command = "curl -X POST https://" + management_address + "/ebs";
        system(shell_command.c_str());
      }
    }

    if (pollitems[3].revents & ZMQ_POLLIN) {
      cerr << "received key hotness update\n";
      string hotness_msg = zmq_util::recv_string(&key_hotness_puller);
      communication::Key_Hotness_Update khu;
      khu.ParseFromString(hotness_msg);

      string node_ip = khu.node_ip();

      for (int i = 0; i < khu.tuple_size(); i++) {
        string key = khu.tuple(i).key();
        size_t access = khu.tuple(i).access();
        key_access_frequency[key][node_ip] = access;
      }
    }

    // TODO: Add policy that triggers node join/removal and key replication factor change

  }
}
