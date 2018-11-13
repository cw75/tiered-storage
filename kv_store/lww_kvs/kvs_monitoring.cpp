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

// number of nodes to add concurrently for storage
#define NODE_ADD 2

using namespace std;
using address_t = string;

// read-only per-tier metadata
unordered_map<unsigned, tier_data> tier_data_map;

void prepare_metadata_get_request(
    string& key,
    global_hash_t& global_memory_hash_ring,
    local_hash_t& local_memory_hash_ring,
    unordered_map<address_t, communication::Request>& addr_request_map,
    monitoring_thread_t& mt,
    unsigned& rid) {
  auto threads = get_responsible_threads_metadata(key, global_memory_hash_ring, local_memory_hash_ring);
  if (threads.size() != 0) {
    string target_address = next(begin(threads), rand() % threads.size())->get_request_pulling_connect_addr();
    if (addr_request_map.find(target_address) == addr_request_map.end()) {
      addr_request_map[target_address].set_type("GET");
      addr_request_map[target_address].set_respond_address(mt.get_request_pulling_connect_addr());
      string req_id = mt.get_ip() + ":" + to_string(rid);
      addr_request_map[target_address].set_request_id(req_id);
      rid += 1;
    }
    prepare_get_tuple(addr_request_map[target_address], key);
  }
}

void prepare_metadata_put_request(
    string& key,
    string& value,
    global_hash_t& global_memory_hash_ring,
    local_hash_t& local_memory_hash_ring,
    unordered_map<address_t, communication::Request>& addr_request_map,
    monitoring_thread_t& mt,
    unsigned& rid) {
  auto threads = get_responsible_threads_metadata(key, global_memory_hash_ring, local_memory_hash_ring);
  if (threads.size() != 0) {
    string target_address = next(begin(threads), rand() % threads.size())->get_request_pulling_connect_addr();
    if (addr_request_map.find(target_address) == addr_request_map.end()) {
      addr_request_map[target_address].set_type("PUT");
      addr_request_map[target_address].set_respond_address(mt.get_request_pulling_connect_addr());
      string req_id = mt.get_ip() + ":" + to_string(rid);
      addr_request_map[target_address].set_request_id(req_id);
      rid += 1;
    }
    prepare_put_tuple(addr_request_map[target_address], key, value, 0);
  }
}

void prepare_replication_factor_update(
    string& key,
    unordered_map<address_t, communication::Replication_Factor_Request>& replication_factor_map,
    string server_address,
    unordered_map<string, key_info>& placement) {
  communication::Replication_Factor_Request_Tuple* tp = replication_factor_map[server_address].add_tuple();
  tp->set_key(key);
  for (auto iter = placement[key].global_replication_map_.begin(); iter != placement[key].global_replication_map_.end(); iter++) {
    communication::Replication_Factor_Request_Global* g = tp->add_global();
    g->set_tier_id(iter->first);
    g->set_global_replication(iter->second);
  }
  for (auto iter = placement[key].local_replication_map_.begin(); iter != placement[key].local_replication_map_.end(); iter++) {
    communication::Replication_Factor_Request_Local* l = tp->add_local();
    l->set_tier_id(iter->first);
    l->set_local_replication(iter->second);
  }
}

// assume the caller has the replication factor for the keys and the requests are valid
// (rep factor <= total number of nodes in a tier)
void change_replication_factor(
    unordered_map<string, key_info>& requests,
    unordered_map<unsigned, global_hash_t>& global_hash_ring_map,
    unordered_map<unsigned, local_hash_t>& local_hash_ring_map,
    vector<address_t>& proxy_address,
    unordered_map<string, key_info>& placement,
    SocketCache& pushers,
    monitoring_thread_t& mt,
    zmq::socket_t& response_puller,
    shared_ptr<spdlog::logger> logger,
    unsigned& rid) {
  // used to keep track of the original replication factors for the requested keys
  unordered_map<string, key_info> orig_placement_info;
  // store the new replication factor synchronously in storage servers
  unordered_map<address_t, communication::Request> addr_request_map;
  // form the placement request map
  unordered_map<address_t, communication::Replication_Factor_Request> replication_factor_map;

  for (auto it = requests.begin(); it != requests.end(); it++) {
    string key = it->first;
    orig_placement_info[key] = placement[key];
    // update the placement map
    for (auto iter = it->second.global_replication_map_.begin(); iter != it->second.global_replication_map_.end(); iter++) {
      placement[key].global_replication_map_[iter->first] = iter->second;
    }
    for (auto iter = it->second.local_replication_map_.begin(); iter != it->second.local_replication_map_.end(); iter++) {
      placement[key].local_replication_map_[iter->first] = iter->second;
    }
    // prepare data to be stored in the storage tier
    communication::Replication_Factor rep_data;
    for (auto iter = placement[key].global_replication_map_.begin(); iter != placement[key].global_replication_map_.end(); iter++) {
      communication::Replication_Factor_Global* g = rep_data.add_global();
      g->set_tier_id(iter->first);
      g->set_global_replication(iter->second);
    }
    for (auto iter = placement[key].local_replication_map_.begin(); iter != placement[key].local_replication_map_.end(); iter++) {
      communication::Replication_Factor_Local* l = rep_data.add_local();
      l->set_tier_id(iter->first);
      l->set_local_replication(iter->second);
    }
    string rep_key = key + "_replication";
    string serialized_rep_data;
    rep_data.SerializeToString(&serialized_rep_data);
    prepare_metadata_put_request(rep_key, serialized_rep_data, global_hash_ring_map[1], local_hash_ring_map[1], addr_request_map, mt, rid);
  }
  // send updates to storage nodes
  unordered_set<string> failed_keys;
  for (auto it = addr_request_map.begin(); it != addr_request_map.end(); it++) {
    bool succeed;
    auto res = send_request<communication::Request, communication::Response>(it->second, pushers[it->first], response_puller, succeed);
    if (!succeed) {
      logger->info("rep factor put timed out!");
      for (int i = 0; i < it->second.tuple_size(); i++) {
        vector<string> tokens;
        split(it->second.tuple(i).key(), '_', tokens);
        failed_keys.insert(tokens[0]);
      }
    } else {
      for (int i = 0; i < res.tuple_size(); i++) {
        if (res.tuple(i).err_number() == 2) {
          logger->info("rep factor put for key {} rejected due to wrong address!", res.tuple(i).key());
          vector<string> tokens;
          split(res.tuple(i).key(), '_', tokens);
          failed_keys.insert(tokens[0]);
        }
      }
    }
  }

  for (auto it = requests.begin(); it != requests.end(); it++) {
    string key = it->first;
    if (failed_keys.find(key) == failed_keys.end()) {
      // prepare data for notifying relevant nodes
      // form rep factor change requests for all tiers
      for (unsigned tier = MIN_TIER; tier <= MAX_TIER; tier++) {
        unsigned rep = max(placement[key].global_replication_map_[tier], orig_placement_info[key].global_replication_map_[tier]);
        auto threads = responsible_global(key, rep, global_hash_ring_map[tier]);
        for (auto server_iter = threads.begin(); server_iter != threads.end(); server_iter++) {
          prepare_replication_factor_update(key, replication_factor_map, server_iter->get_replication_factor_change_connect_addr(), placement);
        }
      }

      // form placement requests for proxy nodes
      for (auto proxy_iter = proxy_address.begin(); proxy_iter != proxy_address.end(); proxy_iter++) {
        prepare_replication_factor_update(key, replication_factor_map, proxy_thread_t(*proxy_iter, 0).get_replication_factor_change_connect_addr(), placement);
      }
    }
  }
  // send placement info update to all relevant nodes
  for (auto it = replication_factor_map.begin(); it != replication_factor_map.end(); it++) {
    string serialized_msg;
    it->second.SerializeToString(&serialized_msg);
    zmq_util::send_string(serialized_msg, &pushers[it->first]);
  }
  // restore rep factor for failed keys
  for (auto it = failed_keys.begin(); it != failed_keys.end(); it++) {
    placement[*it] = orig_placement_info[*it];
  }
}

int main(int argc, char* argv[]) {

  auto logger = spdlog::basic_logger_mt("basic_logger", "log.txt", true);
  logger->flush_on(spdlog::level::info); 

  if (argc != 1) {
    cerr << "usage:" << argv[0] << endl;
    return 1;
  }

  string ip = get_ip("monitoring");

  tier_data_map[1] = tier_data(MEMORY_THREAD_NUM, DEFAULT_GLOBAL_MEMORY_REPLICATION, MEM_NODE_CAPACITY);
  tier_data_map[2] = tier_data(EBS_THREAD_NUM, DEFAULT_GLOBAL_EBS_REPLICATION, EBS_NODE_CAPACITY);

  // initialize hash ring maps
  unordered_map<unsigned, global_hash_t> global_hash_ring_map;
  unordered_map<unsigned, local_hash_t> local_hash_ring_map;

  // form local hash rings
  for (auto it = tier_data_map.begin(); it != tier_data_map.end(); it++) {
    for (unsigned tid = 0; tid < it->second.thread_number_; tid++) {
      insert_to_hash_ring<local_hash_t>(local_hash_ring_map[it->first], ip, tid);
    }
  }

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
  unordered_map<address_t, unordered_map<unsigned, pair<double, unsigned>>> memory_tier_occupancy;
  // keep track of ebs tier thread occupancy
  unordered_map<address_t, unordered_map<unsigned, pair<double, unsigned>>> ebs_tier_occupancy;
  // keep track of memory tier hit
  unordered_map<address_t, unordered_map<unsigned, unsigned>> memory_tier_access;
  // keep track of ebs tier hit
  unordered_map<address_t, unordered_map<unsigned, unsigned>> ebs_tier_access;
  // keep track of user latency info
  unordered_map<address_t, double> user_latency;
  // keep track of user throughput info
  unordered_map<address_t, double> user_throughput;
  // rep factor map
  unordered_map<string, pair<double, unsigned>> rep_factor_map;

  // read in the initial server addresses and build the hash ring
  string ip_line;
  ifstream address;

  vector<address_t> proxy_address;

  // read address of management node from conf file
  address_t management_address;

  address.open("conf/monitoring/management_ip.txt");
  getline(address, ip_line);
  management_address = ip_line;
  address.close();

  monitoring_thread_t mt = monitoring_thread_t(ip);

  zmq::context_t context(1);

  SocketCache pushers(&context, ZMQ_PUSH);

  // responsible for both node join and departure
  zmq::socket_t response_puller(context, ZMQ_PULL);
  int timeout = 10000;
  response_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  response_puller.bind(mt.get_request_pulling_bind_addr());

  unordered_map<address_t, unsigned> departing_node_map;

  // responsible for both node join and departure
  zmq::socket_t notify_puller(context, ZMQ_PULL);
  notify_puller.bind(mt.get_notify_bind_addr());
  // responsible for receiving depart done notice
  zmq::socket_t depart_done_puller(context, ZMQ_PULL);
  depart_done_puller.bind(mt.get_depart_done_bind_addr());
  // responsible for receiving latency update from users
  zmq::socket_t latency_puller(context, ZMQ_PULL);
  latency_puller.bind(mt.get_latency_report_bind_addr());
  // responsible for receiving SLO update
  zmq::socket_t slo_puller(context, ZMQ_PULL);
  slo_puller.bind(mt.get_slo_bind_addr());

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(notify_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(depart_done_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(latency_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(slo_puller), 0, ZMQ_POLLIN, 0 }
  };

  // warm up for benchmark
  logger->info("begin warmup");
  warmup(placement, logger);
  logger->info("finish warmup");

  auto report_start = chrono::system_clock::now();
  auto report_end = chrono::system_clock::now();

  auto grace_start = chrono::system_clock::now();

  unsigned adding_memory_node = 0;
  unsigned adding_ebs_node = 0;
  bool removing_memory_node = false;
  bool removing_ebs_node = false;

  unsigned server_monitoring_epoch = 0;

  unsigned rid = 0;
  bool policy_start = false;

  unsigned slo = SLO;
  double cost_budget = COST_BUDGET;

  while (true) {
    // listen for ZMQ events
    zmq_util::poll(0, &pollitems);

    // handle a join or depart event
    if (pollitems[0].revents & ZMQ_POLLIN) {
      string message = zmq_util::recv_string(&notify_puller);

      vector<string> v;
      split(message, ':', v);
      string type = v[0];
      unsigned tier = stoi(v[1]);
      string new_server_ip = v[2];
      if (type == "join") {
        logger->info("received join");
        logger->info("new server ip is {}", new_server_ip);
        logger->info("tier id is {}", to_string(tier));
        if (tier == 1) {
          insert_to_hash_ring<global_hash_t>(global_hash_ring_map[tier], new_server_ip, 0);
          if (adding_memory_node > 0) {
            adding_memory_node -= 1;
          }
          // reset timer
          grace_start = chrono::system_clock::now();
        } else if (tier == 2) {
          insert_to_hash_ring<global_hash_t>(global_hash_ring_map[tier], new_server_ip, 0);
          if (adding_ebs_node > 0) {
            adding_ebs_node -= 1;
          }
          // reset timer
          grace_start = chrono::system_clock::now();
        } else if (tier == 0) {
          proxy_address.push_back(new_server_ip);
        } else {
          cerr << "Invalid Tier info\n";
        }
        for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
          logger->info("hash ring for tier {} size is {}", to_string(it->first), to_string(it->second.size()));
        }
      } else if (type == "depart") {
        logger->info("received depart");
        logger->info("departing server ip is {}", new_server_ip);
        //cerr << "received depart\n";
        // update hash ring
        if (tier == 1) {
          remove_from_hash_ring<global_hash_t>(global_hash_ring_map[tier], new_server_ip, 0);
          memory_tier_storage.erase(new_server_ip);
          memory_tier_occupancy.erase(new_server_ip);
          for (auto it = key_access_frequency.begin(); it != key_access_frequency.end(); it++) {
            for (unsigned i = 0; i < MEMORY_THREAD_NUM; i++) {
              it->second.erase(new_server_ip + ":" + to_string(i));
            }
          }
        } else if (tier == 2) {
          remove_from_hash_ring<global_hash_t>(global_hash_ring_map[tier], new_server_ip, 0);
          ebs_tier_storage.erase(new_server_ip);
          ebs_tier_occupancy.erase(new_server_ip);
          for (auto it = key_access_frequency.begin(); it != key_access_frequency.end(); it++) {
            for (unsigned i = 0; i < EBS_THREAD_NUM; i++) {
              it->second.erase(new_server_ip + ":" + to_string(i));
            }
          }
        } else {
          cerr << "Invalid Tier info\n";
        }
        for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
          logger->info("hash ring for tier {} size is {}", to_string(it->first), to_string(it->second.size()));
        }
      }
    }

    if (pollitems[1].revents & ZMQ_POLLIN) {
      string msg = zmq_util::recv_string(&depart_done_puller);
      vector<string> tokens;
      split(msg, '_', tokens);
      address_t departed_ip = tokens[0];
      unsigned tier_id = stoi(tokens[1]);
      if (departing_node_map.find(departed_ip) != departing_node_map.end()) {
        departing_node_map[departed_ip] -= 1;
        if (departing_node_map[departed_ip] == 0) {
          if (tier_id == 1) {
            logger->info("removing memory node {}", departed_ip);
            string shell_command = "curl -X POST http://" + management_address + "/remove/memory/" + departed_ip;
            system(shell_command.c_str());
            removing_memory_node = false;
          } else {
            logger->info("removing ebs node {}", departed_ip);
            string shell_command = "curl -X POST http://" + management_address + "/remove/ebs/" + departed_ip;
            system(shell_command.c_str());
            removing_ebs_node = false;
          }
          // reset timer
          grace_start = chrono::system_clock::now();
          departing_node_map.erase(departed_ip);
        }
      } else {
        cerr << "missing entry in the depart done map\n";
      }
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      string serialized_latency = zmq_util::recv_string(&latency_puller);
      communication::Feedback l;
      l.ParseFromString(serialized_latency);
      if (l.has_finish() && l.finish()) {
        user_latency.erase(l.uid());
      } else if (l.has_warmup() && l.warmup()) {
        logger->info("use finished warmup, starting policy engine");
        policy_start = true;
      } else {
        user_latency[l.uid()] = l.latency();
        user_throughput[l.uid()] = l.throughput();
        for (int i = 0; i < l.rep_size(); i++) {
          string key = l.rep(i).key();
          double factor = l.rep(i).factor();
          if (rep_factor_map.find(key) == rep_factor_map.end()) {
            rep_factor_map[key].first = factor;
            rep_factor_map[key].second = 1;
          } else {
            rep_factor_map[key].first = (rep_factor_map[key].first * rep_factor_map[key].second + factor) / (rep_factor_map[key].second + 1);
            rep_factor_map[key].second += 1;
          }
        }
      }
    }

    if (pollitems[3].revents & ZMQ_POLLIN) {
      //todo
      string serialized_slo = zmq_util::recv_string(&slo_puller);
      vector<string> tokens;
      split(serialized_slo, ':', tokens);
      if (tokens[0] == "L") {
        slo = stoi(tokens[1]);
        logger->info("change latency slo to {}", slo);
      } else if (tokens[0] == "C") {
        cost_budget = stod(tokens[1]);
        logger->info("change cost budget to {}", cost_budget);
      } else {
        logger->info("invalid slo type");
      }
    }

    report_end = std::chrono::system_clock::now();

    if (chrono::duration_cast<std::chrono::seconds>(report_end-report_start).count() >= MONITORING_THRESHOLD) {
      server_monitoring_epoch += 1;
      // clear stats
      key_access_frequency.clear();
      key_access_summary.clear();
      memory_tier_storage.clear();
      ebs_tier_storage.clear();
      memory_tier_occupancy.clear();
      ebs_tier_occupancy.clear();

      unordered_map<address_t, communication::Request> addr_request_map;

      for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
        unsigned tier_id = it->first;
        auto hash_ring = &(it->second);
        unordered_set<string> observed_ip;
        for (auto iter = hash_ring->begin(); iter != hash_ring->end(); iter++) {
          if (observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
            for (unsigned i = 0; i < tier_data_map[tier_id].thread_number_; i++) {
              string key = iter->second.get_ip() + "_" + to_string(i) + "_" + to_string(tier_id) + "_stat";
              prepare_metadata_get_request(key, global_hash_ring_map[1], local_hash_ring_map[1], addr_request_map, mt, rid);
              key = iter->second.get_ip() + "_" + to_string(i) + "_" + to_string(tier_id) + "_access";
              prepare_metadata_get_request(key, global_hash_ring_map[1], local_hash_ring_map[1], addr_request_map, mt, rid);
            }
            observed_ip.insert(iter->second.get_ip());
          }
        }
      }

      for (auto it = addr_request_map.begin(); it != addr_request_map.end(); it++) {
        bool succeed;
        auto res = send_request<communication::Request, communication::Response>(it->second, pushers[it->first], response_puller, succeed);
        if (succeed) {
          for (int i = 0; i < res.tuple_size(); i++) {
            if (res.tuple(i).err_number() == 0) {
              vector<string> tokens;
              split(res.tuple(i).key(), '_', tokens);
              string ip = tokens[0];
              unsigned tid = stoi(tokens[1]);
              unsigned tier_id = stoi(tokens[2]);
              string metadata_type = tokens[3];

              if (metadata_type == "stat") {
                // deserialized the value
                communication::Server_Stat stat;
                stat.ParseFromString(res.tuple(i).value());
                if (tier_id == 1) {
                  memory_tier_storage[ip][tid] = stat.storage_consumption();
                  memory_tier_occupancy[ip][tid] = pair<double, unsigned>(stat.occupancy(), stat.epoch());
                  memory_tier_access[ip][tid] = stat.total_access();
                } else {
                  ebs_tier_storage[ip][tid] = stat.storage_consumption();
                  ebs_tier_occupancy[ip][tid] = pair<double, unsigned>(stat.occupancy(), stat.epoch());
                  ebs_tier_access[ip][tid] = stat.total_access();
                }
              } else if (metadata_type == "access") {
                // deserialized the value
                communication::Key_Access access;
                access.ParseFromString(res.tuple(i).value());
                if (tier_id == 1) {
                  for (int j = 0; j < access.tuple_size(); j++) {
                    string key = access.tuple(j).key();
                    key_access_frequency[key][ip + ":" + to_string(tid)] = access.tuple(j).access();
                  }
                } else {
                  for (int j = 0; j < access.tuple_size(); j++) {
                    string key = access.tuple(j).key();
                    key_access_frequency[key][ip + ":" + to_string(tid)] = access.tuple(j).access();
                  }
                }
              }
            } else if (res.tuple(i).err_number() == 1) {
              cerr << "key " + res.tuple(i).key() + " doesn't exist\n";
            } else {
              cerr << "hash ring is inconsistent for key " + res.tuple(i).key() + "\n";
              //TODO: reach here when the hash ring is inconsistent
            }
          }
        } else {
          cerr << "request timed out\n";
          continue;
        }
      }

      // compute key access summary
      unsigned cnt = 0;
      double mean = 0;
      double ms = 0;
      for (auto it = key_access_frequency.begin(); it != key_access_frequency.end(); it++) {
        string key = it->first;
        unsigned total_access = 0;
        for (auto iter = it->second.begin(); iter != it->second.end(); iter++) {
          total_access += iter->second;
        }
        key_access_summary[key] = total_access;

        if (total_access > 0) {
          cnt += 1;
          double delta = total_access - mean;
          mean += (double)delta / cnt;
          double delta2 = total_access - mean;
          ms += delta * delta2;
        }
      }

      double std = sqrt((double)ms / cnt);

      logger->info("access mean is {}", mean);
      logger->info("access var is {}", (double)ms / cnt);
      logger->info("access std is {}", std);

      // compute tier access summary
      unsigned total_memory_access = 0;
      for (auto it = memory_tier_access.begin(); it != memory_tier_access.end(); it++) {
        for (auto iter = it->second.begin(); iter != it->second.end(); iter++) {
          total_memory_access += iter->second;
        }
      }
      unsigned total_ebs_access = 0;
      for (auto it = ebs_tier_access.begin(); it != ebs_tier_access.end(); it++) {
        for (auto iter = it->second.begin(); iter != it->second.end(); iter++) {
          total_ebs_access += iter->second;
        }
      }

      logger->info("total memory access is {}", total_memory_access);
      logger->info("total ebs access is {}", total_ebs_access);

      unsigned long long total_memory_consumption = 0;
      unsigned long long total_ebs_consumption = 0;
      double average_memory_consumption_percentage = 0;
      double average_ebs_consumption_percentage = 0;

      unsigned m_count = 0;
      unsigned e_count = 0;

      double max_memory_consumption_percentage = 0;
      double max_ebs_consumption_percentage = 0;

      for (auto it1 = memory_tier_storage.begin(); it1 != memory_tier_storage.end(); it1++) {
        unsigned total_thread_consumption = 0;
        for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
          total_memory_consumption += it2->second;
          total_thread_consumption += it2->second;
        }
        double percentage = (double)total_thread_consumption / (double)tier_data_map[1].node_capacity_;
        logger->info("memory node {} storage consumption is {}", it1->first, percentage);
        if (percentage > max_memory_consumption_percentage) {
          max_memory_consumption_percentage = percentage;
        }
        m_count += 1;
      }
      for (auto it1 = ebs_tier_storage.begin(); it1 != ebs_tier_storage.end(); it1++) {
        unsigned total_thread_consumption = 0;
        for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
          total_ebs_consumption += it2->second;
          total_thread_consumption += it2->second;
        }
        double percentage = (double)total_thread_consumption / (double)tier_data_map[2].node_capacity_;
        logger->info("ebs node {} storage consumption is {}", it1->first, percentage);
        if (percentage > max_ebs_consumption_percentage) {
          max_ebs_consumption_percentage = percentage;
        }
        e_count += 1;
      }
      if (m_count != 0) {
        average_memory_consumption_percentage = (double)total_memory_consumption / ((double)m_count * tier_data_map[1].node_capacity_);
        logger->info("average memory node consumption percentage is {}", average_memory_consumption_percentage);
        logger->info("max memory node consumption percentage is {}", max_memory_consumption_percentage);
      }
      if (e_count != 0) {
        average_ebs_consumption_percentage = (double)total_ebs_consumption / ((double)e_count * tier_data_map[2].node_capacity_);
        logger->info("average ebs node consumption percentage is {}", average_ebs_consumption_percentage);
        logger->info("max ebs node consumption percentage is {}", max_ebs_consumption_percentage);
      }

      double max_memory_occupancy = 0.0;
      double min_memory_occupancy = 1.0;
      double sum_memory_occupancy = 0.0;
      string min_node_ip;
      unsigned count = 0;
      for (auto it1 = memory_tier_occupancy.begin(); it1 != memory_tier_occupancy.end(); it1++) {
        double sum_thread_occupancy = 0.0;
        unsigned thread_count = 0;
        for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
          logger->info("memory node ip {} thread {} occupancy is {} at server epoch {} for monitoring epoch {}", it1->first, it2->first, it2->second.first, it2->second.second, server_monitoring_epoch);
          sum_thread_occupancy += it2->second.first;
          thread_count += 1;
        }
        double node_occupancy = sum_thread_occupancy / thread_count;
        sum_memory_occupancy += node_occupancy;
        if (node_occupancy > max_memory_occupancy) {
          max_memory_occupancy = node_occupancy;
        }
        if (node_occupancy < min_memory_occupancy) {
          min_memory_occupancy = node_occupancy;
          min_node_ip = it1->first;
        }
        count += 1;
      }
      double avg_memory_occupancy = sum_memory_occupancy / count;
      logger->info("max memory node occupancy is {}", to_string(max_memory_occupancy));
      logger->info("min memory node occupancy is {}", to_string(min_memory_occupancy));
      logger->info("avg memory node occupancy is {}", to_string(avg_memory_occupancy));

      double max_ebs_occupancy = 0.0;
      double min_ebs_occupancy = 1.0;
      double sum_ebs_occupancy = 0.0;
      count = 0;
      for (auto it1 = ebs_tier_occupancy.begin(); it1 != ebs_tier_occupancy.end(); it1++) {
        double sum_thread_occupancy = 0.0;
        unsigned thread_count = 0;
        for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
          logger->info("ebs node ip {} thread {} occupancy is {} at server epoch {} for monitoring epoch {}", it1->first, it2->first, it2->second.first, it2->second.second, server_monitoring_epoch);
          sum_thread_occupancy += it2->second.first;
          thread_count += 1;
        }
        double node_occupancy = sum_thread_occupancy / thread_count;
        sum_ebs_occupancy += node_occupancy;
        if (node_occupancy > max_ebs_occupancy) {
          max_ebs_occupancy = node_occupancy;
        }
        if (node_occupancy < min_ebs_occupancy) {
          min_ebs_occupancy = node_occupancy;
        }
        count += 1;
      }
      double avg_ebs_occupancy = sum_ebs_occupancy / count;
      logger->info("max ebs node occupancy is {}", to_string(max_ebs_occupancy));
      logger->info("min ebs node occupancy is {}", to_string(min_ebs_occupancy));
      logger->info("avg ebs node occupancy is {}", to_string(avg_ebs_occupancy));

      // gather latency info
      double avg_latency = 0;
      if (user_latency.size() > 0) {
        // compute latency from users
        logger->info("computing latency from user feedback");
        double sum_latency = 0;
        unsigned count = 0;
        for (auto it = user_latency.begin(); it != user_latency.end(); it++) {
          sum_latency += it->second;
          count += 1;
        }
        avg_latency = sum_latency / count;
      }
      logger->info("avg latency is {}", avg_latency);
      // gather throughput info
      double total_throughput = 0;
      if (user_throughput.size() > 0) {
        // compute latency from users
        logger->info("computing throughput from user feedback");
        for (auto it = user_throughput.begin(); it != user_throughput.end(); it++) {
          total_throughput += it->second;
        }
      }
      logger->info("total throughput is {}", total_throughput);

      //logger->info("logging suggested rep factor change");
      /*for (auto it = rep_factor_map.begin(); it != rep_factor_map.end(); it++) {
        logger->info("suggested factor for key {} is {}", it->first, it->second.first);
      }*/

      unsigned required_memory_node = ceil(total_memory_consumption / (MEM_CAPACITY_MAX * tier_data_map[1].node_capacity_));
      unsigned required_ebs_node = ceil(total_ebs_consumption / (EBS_CAPACITY_MAX * tier_data_map[2].node_capacity_));
      logger->info("required memory node is {}", required_memory_node);
      logger->info("required ebs node is {}", required_ebs_node);

      unsigned memory_node_number = global_hash_ring_map[1].size() / VIRTUAL_THREAD_NUM;
      unsigned ebs_node_number = global_hash_ring_map[2].size() / VIRTUAL_THREAD_NUM;

      // Policy Start Here:
      if (true) {
        unordered_map<string, key_info> requests;
        unsigned total_rep_to_change = 0;

        // 2. check key access summary to promote hot keys to memory tier
        unsigned slot = (MEM_CAPACITY_MAX * tier_data_map[1].node_capacity_ * memory_node_number - total_memory_consumption) / (double)VALUE_SIZE;
        bool overflow = false;
        for (auto it = key_access_summary.begin(); it != key_access_summary.end(); it++) {
          string key = it->first;
          unsigned total_access = it->second;
          if (placement.find(key) == placement.end()) {
            placement[key].global_replication_map_[1] = DEFAULT_GLOBAL_MEMORY_REPLICATION;
            placement[key].global_replication_map_[2] = DEFAULT_GLOBAL_EBS_REPLICATION;
            placement[key].local_replication_map_[1] = DEFAULT_LOCAL_REPLICATION;
            placement[key].local_replication_map_[2] = DEFAULT_LOCAL_REPLICATION;
          }
          if (!is_metadata(key) && total_access > PROMOTE_THRESHOLD && placement[key].global_replication_map_[1] == 0) {
            total_rep_to_change += 1;
            if (total_rep_to_change > slot) {
              overflow = true;
            } else {
              key_info new_rep_factor;
              new_rep_factor.global_replication_map_[1] = placement[key].global_replication_map_[1] + 1;
              new_rep_factor.global_replication_map_[2] = placement[key].global_replication_map_[2] - 1;
              requests[key] = new_rep_factor;
            }
          }
        }
        change_replication_factor(requests, global_hash_ring_map, local_hash_ring_map, proxy_address, placement, pushers, mt, response_puller, logger, rid);
        logger->info("number of keys to be promoted is {}", total_rep_to_change);
        logger->info("available memory slot is {}", slot);
        auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-grace_start).count();
        if (overflow && adding_memory_node == 0 && time_elapsed > GRACE_PERIOD) {
          unsigned long long promote_data_size = total_rep_to_change * (double)VALUE_SIZE;
          unsigned total_memory_node_needed = ceil((total_memory_consumption + promote_data_size) / (MEM_CAPACITY_MAX * tier_data_map[1].node_capacity_));
          if (total_memory_node_needed > memory_node_number) {
            logger->info("memory node insufficient to promote keys!");
            unsigned node_to_add = ceil(1.5 * (total_memory_node_needed - memory_node_number));
            logger->info("trigger add {} memory node", to_string(node_to_add));
            string shell_command = "curl -X POST http://" + management_address + "/add/memory/" + to_string(node_to_add) + " &";
            system(shell_command.c_str());
            adding_memory_node = node_to_add;
          }
        } else if (overflow) {
          logger->info("in grace period or adding nodes");
        }

        requests.clear();
        total_rep_to_change = 0;
      } else {
        logger->info("policy not started");
      }

      user_latency.clear();
      user_throughput.clear();
      rep_factor_map.clear();
      
      report_start = std::chrono::system_clock::now();
    }
  }
}
