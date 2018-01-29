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

void prepare_metadata_get_request(
    string key,
    global_hash_t& global_memory_hash_ring,
    unordered_map<address_t, communication::Request>& addr_request_map) {
  auto threads = responsible_global(key, METADATA_MEMORY_REPLICATION_FACTOR, global_memory_hash_ring);
  if (threads.size() != 0) {
    string target_address = next(begin(threads), rand() % threads.size())->get_request_handling_connect_addr();
    if (addr_request_map.find(target_address) == addr_request_map.end()) {
      addr_request_map[target_address].set_type("GET");
    }
    prepare_get_tuple(addr_request_map[target_address], key);
  }
}

void prepare_metadata_put_request(
    string key,
    string value,
    global_hash_t& global_memory_hash_ring,
    unordered_map<address_t, communication::Request>& addr_request_map) {
  auto threads = responsible_global(key, METADATA_MEMORY_REPLICATION_FACTOR, global_memory_hash_ring);
  if (threads.size() != 0) {
    string target_address = next(begin(threads), rand() % threads.size())->get_request_pulling_connect_addr();
    if (addr_request_map.find(target_address) == addr_request_map.end()) {
      addr_request_map[target_address].set_type("PUT");
    }
    prepare_put_tuple(addr_request_map[target_address], key, value, 0);
  }
}

void prepare_replication_factor_update(
    string key,
    unordered_map<address_t, communication::Replication_Factor_Request>& replication_factor_map,
    string server_address,
    unordered_map<string, key_info>& placement,
    unordered_map<address_t, unsigned>& local_replication_map) {
  communication::Replication_Factor_Request_Tuple* tp = replication_factor_map[server_address].add_tuple();
  tp->set_key(key);
  tp->set_global_memory_replication(placement[key].global_memory_replication_);
  tp->set_global_ebs_replication(placement[key].global_ebs_replication_);
  for (auto iter = local_replication_map.begin(); iter != local_replication_map.end(); iter++) {
    communication::Replication_Factor_Request_Local* l = tp->add_local();
    l->set_ip(iter->first);
    l->set_local_replication(iter->second);
  }
}

// assume the caller has the replication factor for the keys and the requests are valid
// (rep factor <= total number of nodes in a tier)
void change_replication_factor(
    unordered_map<string, key_info>& requests,
    global_hash_t& global_memory_hash_ring,
    global_hash_t& global_ebs_hash_ring,
    vector<address_t>& proxy_address,
    unordered_map<string, key_info>& placement,
    SocketCache& pushers) {
  // used to keep track of the original replication factors for the requested keys
  unordered_map<string, key_info> orig_placement_info;

  // form the placement request map
  unordered_map<address_t, communication::Replication_Factor_Request> replication_factor_map;

  for (auto it = requests.begin(); it != requests.end(); it++) {
    string key = it->first;
    orig_placement_info[key] = placement[key];
    // update the placement map
    placement[key].global_memory_replication_ = it->second.global_memory_replication_;
    placement[key].global_ebs_replication_ = it->second.global_ebs_replication_;
    for (auto iter = it->second.local_replication_.begin(); iter != it->second.local_replication_.end(); iter++) {
      placement[key].local_replication_[iter->first] = iter->second;
    }
    // for each tier, take the max between the key's original rep factor and current rep factor
    int memory_rep = max(placement[key].global_memory_replication_, orig_placement_info[key].global_memory_replication_);
    int ebs_rep = max(placement[key].global_ebs_replication_, orig_placement_info[key].global_ebs_replication_);

    // form placement requests for memory tier nodes
    auto server_iter = global_memory_hash_ring.find(key);
    if (server_iter != global_memory_hash_ring.end()) {
      for (int i = 0; i < memory_rep; i++) {
        prepare_replication_factor_update(key, replication_factor_map, server_iter->second.get_replication_factor_connect_addr(), placement, it->second.local_replication_);

        if (++server_iter == global_memory_hash_ring.end()) {
          server_iter = global_memory_hash_ring.begin();
        }
      }
    }
    // form placement requests for ebs tier nodes
    server_iter = global_ebs_hash_ring.find(key);
    if (server_iter != global_ebs_hash_ring.end()) {
      for (int i = 0; i < ebs_rep; i++) {
        prepare_replication_factor_update(key, replication_factor_map, server_iter->second.get_replication_factor_connect_addr(), placement, it->second.local_replication_);

        if (++server_iter == global_ebs_hash_ring.end()) {
          server_iter = global_ebs_hash_ring.begin();
        }
      }
    }
    // in case only the local replication factor is changed for some nodes
    for (auto iter = it->second.local_replication_.begin(); iter != it->second.local_replication_.end(); iter++) {
      // "M" or "E" doesn't matter
      prepare_replication_factor_update(key, replication_factor_map, server_thread_t(iter->first, 0, "M").get_replication_factor_connect_addr(), placement, it->second.local_replication_);
    }

    // form placement requests for proxy nodes
    for (auto proxy_iter = proxy_address.begin(); proxy_iter != proxy_address.end(); proxy_iter++) {
      prepare_replication_factor_update(key, replication_factor_map, proxy_thread_t(*proxy_iter, 0).get_replication_factor_connect_addr(), placement, it->second.local_replication_);
    }
  }

  // send placement info update to all relevant server nodes
  for (auto it = replication_factor_map.begin(); it != replication_factor_map.end(); it++) {
    string serialized_msg;
    it->second.SerializeToString(&serialized_msg);
    zmq_util::send_string(serialized_msg, &pushers[it->first]);
  }

  // store the new replication factor in storage servers
  unordered_map<address_t, communication::Request> addr_request_map;
  for (auto it = requests.begin(); it != requests.end(); it++) {
    string key_global = it->first + "_replication";
    string global_replication_value = to_string(it->second.global_memory_replication_) + ":" + to_string(it->second.global_ebs_replication_);
    prepare_metadata_put_request(key_global, global_replication_value, global_memory_hash_ring, addr_request_map);
    for (auto iter = it->second.local_replication_.begin(); iter != it->second.local_replication_.end(); iter++) {
      string key_local = it->first + "_" + iter->first + "_replication";
      string local_replication_value = to_string(iter->second);
      prepare_metadata_put_request(key_local, local_replication_value, global_memory_hash_ring, addr_request_map);
    }
  }

  for (auto it = addr_request_map.begin(); it != addr_request_map.end(); it++) {
    push_request(it->second, pushers[it->first]);
  }
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
  // keep track of the keys' access by worker address
  unordered_map<string, unordered_map<address_t, unsigned>> key_access_frequency;
  // keep track of the keys' access summary
  unordered_map<string, unsigned> key_access_summary;
  // keep track of memory tier storage consumption
  unordered_map<address_t, unordered_map<unsigned, unsigned long long>> memory_tier_storage;
  // keep track of ebs tier storage consumption
  unordered_map<address_t, unordered_map<unsigned, unsigned long long>> ebs_tier_storage;
  // keep track of memory tier thread occupancy
  unordered_map<address_t, unordered_map<unsigned, double>> memory_tier_occupancy;
  // keep track of ebs tier thread occupancy
  unordered_map<address_t, unordered_map<unsigned, double>> ebs_tier_occupancy;
  // read in the initial server addresses and build the hash ring
  string ip_line;
  ifstream address;

  vector<address_t> proxy_address;

  // read address of management node from conf file
  /*address_t management_address;

  address.open("conf/monitoring/management_ip.txt");
  getline(address, ip_line);
  management_address = ip_line;
  address.close();*/

  zmq::context_t context(1);

  SocketCache pushers(&context, ZMQ_PUSH);
  SocketCache requesters(&context, ZMQ_REQ);

  // responsible for both node join and departure
  zmq::socket_t notify_puller(context, ZMQ_PULL);
  notify_puller.bind("tcp://*:" + to_string(NOTIFY_BASE_PORT));

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(notify_puller), 0, ZMQ_POLLIN, 0 }
  };

  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  bool adding_memory_node = false;
  bool adding_ebs_node = false;

  unsigned server_monitoring_epoch = 0;

  double average_memory_consumption = 0;
  double average_ebs_consumption = 0;
  double average_memory_consumption_new = 0;
  double average_ebs_consumption_new = 0;

  while (true) {
    // listen for ZMQ events
    zmq_util::poll(0, &pollitems);

    // handle a join or depart event
    if (pollitems[0].revents & ZMQ_POLLIN) {
      vector<string> v;
      split(zmq_util::recv_string(&notify_puller), ':', v);
      if (v[0] == "join") {
        logger->info("received join");
        // update hash ring
        if (v[1] == "M") {
          global_memory_hash_ring.insert(server_thread_t(v[2], 0, "M"));
          adding_memory_node = false;
          // reset storage timer
          report_start = std::chrono::system_clock::now();
          report_end = std::chrono::system_clock::now();
        } else if (v[1] == "E") {
          global_ebs_hash_ring.insert(server_thread_t(v[2], 0, "E"));
          adding_ebs_node = false;
          // reset storage timer
          report_start = std::chrono::system_clock::now();
          report_end = std::chrono::system_clock::now();
        } else if (v[1] == "P") {
          proxy_address.push_back(v[2]);
        } else {
          cerr << "Invalid Tier info\n";
        }
        logger->info("memory hash ring size is {}", to_string(global_memory_hash_ring.size()));
        logger->info("ebs hash ring size is {}", to_string(global_ebs_hash_ring.size()));
      } else if (v[0] == "depart") {
        logger->info("received depart");
        //cerr << "received depart\n";
        // update hash ring
        if (v[1] == "M") {
          global_memory_hash_ring.erase(server_thread_t(v[2], 0, "M"));
          memory_tier_storage.erase(v[2]);
          memory_tier_occupancy.erase(v[2]);
          // TODO: also clear key access frequency
        } else if (v[1] == "E") {
          global_ebs_hash_ring.erase(server_thread_t(v[2], 0, "E"));
          ebs_tier_storage.erase(v[2]);
          ebs_tier_occupancy.erase(v[2]);
          // TODO: also clear key access frequency
        } else {
          cerr << "Invalid Tier info\n";
        }
        logger->info("memory hash ring size is {}", to_string(global_memory_hash_ring.size()));
        logger->info("ebs hash ring size is {}", to_string(global_ebs_hash_ring.size()));
      }
    }

    report_end = std::chrono::system_clock::now();

    if (chrono::duration_cast<std::chrono::microseconds>(report_end-report_start).count() >= SERVER_REPORT_THRESHOLD) {
      server_monitoring_epoch += 1;

      unordered_map<address_t, communication::Request> addr_request_map;
      string target_address;

      for (auto it = global_memory_hash_ring.begin(); it != global_memory_hash_ring.end(); it++) {
        for (unsigned i = 0; i <= MEMORY_THREAD_NUM; i++) {
          string key = it->second.get_ip() + "_" + to_string(i) + "_M_stat";
          prepare_metadata_get_request(key, global_memory_hash_ring, addr_request_map);
          key = it->second.get_ip() + "_" + to_string(i) + "_M_access";
          prepare_metadata_get_request(key, global_memory_hash_ring, addr_request_map);
        }
      }

      for (auto it = global_ebs_hash_ring.begin(); it != global_ebs_hash_ring.end(); it++) {
        for (unsigned i = 1; i <= EBS_THREAD_NUM; i++) {
          string key = it->second.get_ip() + "_" + to_string(i) + "_E_stat";
          prepare_metadata_get_request(key, global_memory_hash_ring, addr_request_map);
          key = it->second.get_ip() + "_" + to_string(i) + "_E_access";
          prepare_metadata_get_request(key, global_memory_hash_ring, addr_request_map);
        }
      }

      for (auto it = addr_request_map.begin(); it != addr_request_map.end(); it++) {
        auto res = send_request<communication::Request, communication::Response>(it->second, requesters[it->first]);
        for (int i = 0; i < res.tuple_size(); i++) {
          if (res.tuple(i).err_number() == 0) {
            vector<string> tokens;
            split(res.tuple(i).key(), '_', tokens);
            string ip = tokens[0];
            unsigned tid = stoi(tokens[1]);
            string node_type = tokens[2];
            string metadata_type = tokens[3];

            if (metadata_type == "stat") {
              // deserialized the value
              communication::Server_Stat stat;
              stat.ParseFromString(res.tuple(i).value());
              if (node_type == "M") {
                memory_tier_storage[ip][tid] = stat.storage_consumption();
                memory_tier_occupancy[ip][tid] = stat.occupancy();
              } else {
                ebs_tier_storage[ip][tid] = stat.storage_consumption();
                ebs_tier_occupancy[ip][tid] = stat.occupancy();
              }
            } else if (metadata_type == "access") {
              // deserialized the value
              communication::Key_Access access;
              access.ParseFromString(res.tuple(i).value());
              for (int j = 0; j < access.tuple_size(); j++) {
                key_access_frequency[access.tuple(j).key()][ip + ":" + to_string(tid)] = access.tuple(j).access();
              }
            }
          } else if (res.tuple(i).err_number() == 1) {
            cerr << "key " + res.tuple(i).key() + " doesn't exist\n";
          } else {
            cerr << "hash ring is inconsistent for key " + res.tuple(i).key() + "\n";
            //TODO: reach here when the hash ring is inconsistent
          }
        }
      }

      unsigned long long total_memory_consumption = 0;
      unsigned long long total_ebs_consumption = 0;
      unsigned memory_node_count = 0;
      unsigned ebs_volume_count = 0;

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

      if (memory_node_count != 0) {
        average_memory_consumption_new = (double)total_memory_consumption / (double)memory_node_count;
        if (average_memory_consumption != average_memory_consumption_new) {
          logger->info("avg memory consumption for epoch {} is {:03.3f}", server_monitoring_epoch, average_memory_consumption_new);
          average_memory_consumption = average_memory_consumption_new;
        }
      }

      if (ebs_volume_count != 0) {
        average_ebs_consumption_new = (double)total_ebs_consumption / (double)ebs_volume_count;
        if (average_ebs_consumption != average_ebs_consumption_new) {
          logger->info("avg ebs consumption for epoch {} is {:03.3f}", server_monitoring_epoch, average_ebs_consumption_new);
          average_ebs_consumption = average_ebs_consumption_new;
        }
      }

      /*if (average_memory_consumption >= 100 && !adding_memory_node) {
        logger->info("trigger add memory node");
        //cerr << "trigger add memory node\n";
        string shell_command = "curl -X POST http://" + management_address + "/memory";
        system(shell_command.c_str());
        adding_memory_node = true;
      }

      if (average_ebs_consumption >= 500 && !adding_ebs_node) {
        logger->info("trigger add ebs node");
        //cerr << "trigger add ebs node\n";
        string shell_command = "curl -X POST http://" + management_address + "/ebs";
        system(shell_command.c_str());
        adding_ebs_node = true;
      }*/

      if (server_monitoring_epoch % 50 == 1) {
        for (auto it1 = memory_tier_occupancy.begin(); it1 != memory_tier_occupancy.end(); it1++) {
          for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
            logger->info("memory node ip {} thread {} occupancy is {} for epoch {}", it1->first, it2->first, it2->second, server_monitoring_epoch);
          }
        }

        for (auto it1 = ebs_tier_occupancy.begin(); it1 != ebs_tier_occupancy.end(); it1++) {
          for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
            logger->info("ebs node ip {} thread {} occupancy is {} for epoch {}", it1->first, it2->first, it2->second, server_monitoring_epoch);
          }
        }
      }

      report_start = std::chrono::system_clock::now();
    }
  }
}
