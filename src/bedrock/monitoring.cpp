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
#include "spdlog/spdlog.h"
#include "communication.pb.h"
#include "zmq/socket_cache.h"
#include "zmq/zmq_util.h"
#include "utils/consistent_hash_map.hpp"
#include "common.h"
#include "threads.h"
#include "requests.h"
#include "hashers.h"
#include "hash_ring.h"
#include "yaml-cpp/yaml.h"

// the default number of nodes to add concurrently for storage
#define NODE_ADD 2

using namespace std;
using address_t = string;

// read-only per-tier metadata
unordered_map<unsigned, TierData> tier_data_map;

struct SummaryStats {
  void clear() {
    key_access_mean = 0;
    key_access_std = 0;
    total_memory_access = 0;
    total_ebs_access = 0;
    total_memory_consumption = 0;
    total_ebs_consumption = 0;
    max_memory_consumption_percentage = 0;
    max_ebs_consumption_percentage = 0;
    avg_memory_consumption_percentage = 0;
    avg_ebs_consumption_percentage = 0;
    required_memory_node = 0;
    required_ebs_node = 0;
    max_memory_occupancy = 0;
    min_memory_occupancy = 1;
    avg_memory_occupancy = 0;
    max_ebs_occupancy = 0;
    min_ebs_occupancy = 1;
    avg_ebs_occupancy = 0;
    min_occupancy_memory_ip = string();
    avg_latency = 0;
    total_throughput = 0;
  }
  SummaryStats() {
    clear();
  }
  double key_access_mean;
  double key_access_std;
  unsigned total_memory_access;
  unsigned total_ebs_access;
  unsigned long long total_memory_consumption;
  unsigned long long total_ebs_consumption;
  double max_memory_consumption_percentage;
  double max_ebs_consumption_percentage;
  double avg_memory_consumption_percentage;
  double avg_ebs_consumption_percentage;
  unsigned required_memory_node;
  unsigned required_ebs_node;
  double max_memory_occupancy;
  double min_memory_occupancy;
  double avg_memory_occupancy;
  double max_ebs_occupancy;
  double min_ebs_occupancy;
  double avg_ebs_occupancy;
  string min_occupancy_memory_ip;
  double avg_latency;
  double total_throughput;
};

string prepare_metadata_request(
    string& key,
    GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring,
    unordered_map<address_t, communication::Request>& addr_request_map,
    MonitoringThread& mt,
    unsigned& rid,
    string value) {

  auto threads = get_responsible_threads_metadata(key, global_memory_hash_ring, local_memory_hash_ring);
  if (threads.size() != 0) {
    string target_address = next(begin(threads), rand() % threads.size())->get_request_pulling_connect_addr();
    if (addr_request_map.find(target_address) == addr_request_map.end()) {
      addr_request_map[target_address].set_type(value);
      addr_request_map[target_address].set_respond_address(mt.get_request_pulling_connect_addr());
      string req_id = mt.get_ip() + ":" + to_string(rid);
      addr_request_map[target_address].set_request_id(req_id);
      rid += 1;
    }

    return target_address;
  }

  return string();
}

void prepare_metadata_get_request(
    string& key,
    GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring,
    unordered_map<address_t, communication::Request>& addr_request_map,
    MonitoringThread& mt,
    unsigned& rid) {

  string target_address = prepare_metadata_request(key, global_memory_hash_ring, local_memory_hash_ring, addr_request_map, mt, rid, "GET");

  if (!target_address.empty()) {
    prepare_get_tuple(addr_request_map[target_address], key);
  }
}

void prepare_metadata_put_request(
    string& key,
    string& value,
    GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring,
    unordered_map<address_t, communication::Request>& addr_request_map,
    MonitoringThread& mt,
    unsigned& rid) {

  string target_address = prepare_metadata_request(key, global_memory_hash_ring, local_memory_hash_ring, addr_request_map, mt, rid, "PUT");

  if (!target_address.empty()) {
    prepare_put_tuple(addr_request_map[target_address], key, value, 0);
  }
}

void collect_internal_stats(
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    SocketCache& pushers,
    MonitoringThread& mt,
    zmq::socket_t& response_puller,
    shared_ptr<spdlog::logger> logger,
    unsigned& rid,
    unordered_map<string, unordered_map<address_t, unsigned>>& key_access_frequency,
    unordered_map<address_t, unordered_map<unsigned, unsigned long long>>& memory_tier_storage,
    unordered_map<address_t, unordered_map<unsigned, unsigned long long>>& ebs_tier_storage,
    unordered_map<address_t, unordered_map<unsigned, pair<double, unsigned>>>& memory_tier_occupancy,
    unordered_map<address_t, unordered_map<unsigned, pair<double, unsigned>>>& ebs_tier_occupancy,
    unordered_map<address_t, unordered_map<unsigned, unsigned>>& memory_tier_access,
    unordered_map<address_t, unordered_map<unsigned, unsigned>>& ebs_tier_access) {

  unordered_map<address_t, communication::Request> addr_request_map;

  for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
    unsigned tier_id = it->first;
    auto hash_ring = &(it->second);
    unordered_set<string> observed_ip;

    for (auto iter = hash_ring->begin(); iter != hash_ring->end(); iter++) {
      if (observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
        for (unsigned i = 0; i < tier_data_map[tier_id].thread_number_; i++) {
          string key = string(METADATA_IDENTIFIER) + "_" + iter->second.get_ip() + "_" + to_string(i) + "_" + to_string(tier_id) + "_stat";
          prepare_metadata_get_request(key, global_hash_ring_map[1], local_hash_ring_map[1], addr_request_map, mt, rid);

          key = string(METADATA_IDENTIFIER) + "_" + iter->second.get_ip() + "_" + to_string(i) + "_" + to_string(tier_id) + "_access";
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
          logger->error("Key {} doesn't exist.", res.tuple(i).key());
        } else {
          //The hash ring should never be inconsistent.
          logger->error("Hash ring is inconsistent for key {}.", res.tuple(i).key());
        }
      }
    } else {
      logger->error("Request timed out.");
      continue;
    }
  }
}

void compute_summary_stats(
    unordered_map<string, unordered_map<address_t, unsigned>>& key_access_frequency,
    unordered_map<address_t, unordered_map<unsigned, unsigned long long>>& memory_tier_storage,
    unordered_map<address_t, unordered_map<unsigned, unsigned long long>>& ebs_tier_storage,
    unordered_map<address_t, unordered_map<unsigned, pair<double, unsigned>>>& memory_tier_occupancy,
    unordered_map<address_t, unordered_map<unsigned, pair<double, unsigned>>>& ebs_tier_occupancy,
    unordered_map<address_t, unordered_map<unsigned, unsigned>>& memory_tier_access,
    unordered_map<address_t, unordered_map<unsigned, unsigned>>& ebs_tier_access,
    unordered_map<string, unsigned>& key_access_summary,
    SummaryStats& ss,
    shared_ptr<spdlog::logger> logger,
    unsigned& server_monitoring_epoch) {
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

  ss.key_access_mean = mean;
  ss.key_access_std = sqrt((double)ms / cnt);

  logger->info("Access: mean={}, std={}", ss.key_access_mean, ss.key_access_std);

  // compute tier access summary
  for (auto it = memory_tier_access.begin(); it != memory_tier_access.end(); it++) {
    for (auto iter = it->second.begin(); iter != it->second.end(); iter++) {
      ss.total_memory_access += iter->second;
    }
  }

  for (auto it = ebs_tier_access.begin(); it != ebs_tier_access.end(); it++) {
    for (auto iter = it->second.begin(); iter != it->second.end(); iter++) {
      ss.total_ebs_access += iter->second;
    }
  }

  logger->info("Total accesses: memory={}, ebs={}", ss.total_memory_access, ss.total_ebs_access);

  // compute storage consumption related statistics
  unsigned m_count = 0;
  unsigned e_count = 0;

  for (auto it1 = memory_tier_storage.begin(); it1 != memory_tier_storage.end(); it1++) {
    unsigned total_thread_consumption = 0;

    for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
      ss.total_memory_consumption += it2->second;
      total_thread_consumption += it2->second;
    }

    double percentage = (double)total_thread_consumption / (double)tier_data_map[1].node_capacity_;
    logger->info("Memory node {} storage consumption is {}.", it1->first, percentage);

    if (percentage > ss.max_memory_consumption_percentage) {
      ss.max_memory_consumption_percentage = percentage;
    }

    m_count += 1;
  }

  for (auto it1 = ebs_tier_storage.begin(); it1 != ebs_tier_storage.end(); it1++) {
    unsigned total_thread_consumption = 0;

    for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
      ss.total_ebs_consumption += it2->second;
      total_thread_consumption += it2->second;
    }

    double percentage = (double)total_thread_consumption / (double)tier_data_map[2].node_capacity_;
    logger->info("EBS node {} storage consumption is {}.", it1->first, percentage);

    if (percentage > ss.max_ebs_consumption_percentage) {
      ss.max_ebs_consumption_percentage = percentage;
    }
    e_count += 1;
  }

  if (m_count != 0) {
    ss.avg_memory_consumption_percentage = (double)ss.total_memory_consumption / ((double)m_count * tier_data_map[1].node_capacity_);
    logger->info("Average memory node consumption is {}.", ss.avg_memory_consumption_percentage);
    logger->info("Max memory node consumption is {}.", ss.max_memory_consumption_percentage);
  }

  if (e_count != 0) {
    ss.avg_ebs_consumption_percentage = (double)ss.total_ebs_consumption / ((double)e_count * tier_data_map[2].node_capacity_);
    logger->info("Average EBS node consumption is {}.", ss.avg_ebs_consumption_percentage);
    logger->info("Max EBS node consumption is {}.", ss.max_ebs_consumption_percentage);
  }

  ss.required_memory_node = ceil(ss.total_memory_consumption / (MEM_CAPACITY_MAX * tier_data_map[1].node_capacity_));
  ss.required_ebs_node = ceil(ss.total_ebs_consumption / (EBS_CAPACITY_MAX * tier_data_map[2].node_capacity_));

  logger->info("The system requires {} new memory nodes.", ss.required_memory_node);
  logger->info("The system requires {} new EBS nodes.", ss.required_ebs_node);

  // compute occupancy related statistics
  double sum_memory_occupancy = 0.0;

  unsigned count = 0;

  for (auto it1 = memory_tier_occupancy.begin(); it1 != memory_tier_occupancy.end(); it1++) {
    double sum_thread_occupancy = 0.0;
    unsigned thread_count = 0;

    for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
      logger->info("Memory node {} thread {} occupancy is {} at epoch {} (monitoring epoch {}).", it1->first, it2->first, it2->second.first, it2->second.second, server_monitoring_epoch);
      sum_thread_occupancy += it2->second.first;
      thread_count += 1;
    }

    double node_occupancy = sum_thread_occupancy / thread_count;
    sum_memory_occupancy += node_occupancy;

    if (node_occupancy > ss.max_memory_occupancy) {
      ss.max_memory_occupancy = node_occupancy;
    }

    if (node_occupancy < ss.min_memory_occupancy) {
      ss.min_memory_occupancy = node_occupancy;
      ss.min_occupancy_memory_ip = it1->first;
    }

    count += 1;
  }

  ss.avg_memory_occupancy = sum_memory_occupancy / count;
  logger->info("Max memory node occupancy is {}.", to_string(ss.max_memory_occupancy));
  logger->info("Min memory node occupancy is {}.", to_string(ss.min_memory_occupancy));
  logger->info("Average memory node occupancy is {}.", to_string(ss.avg_memory_occupancy));

  double sum_ebs_occupancy = 0.0;

  count = 0;

  for (auto it1 = ebs_tier_occupancy.begin(); it1 != ebs_tier_occupancy.end(); it1++) {
    double sum_thread_occupancy = 0.0;
    unsigned thread_count = 0;

    for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
      logger->info("EBS node {} thread {} occupancy is {} at epoch {} (monitoring epoch {}).", it1->first, it2->first, it2->second.first, it2->second.second, server_monitoring_epoch);
      sum_thread_occupancy += it2->second.first;
      thread_count += 1;
    }

    double node_occupancy = sum_thread_occupancy / thread_count;
    sum_ebs_occupancy += node_occupancy;

    if (node_occupancy > ss.max_ebs_occupancy) {
      ss.max_ebs_occupancy = node_occupancy;
    }

    if (node_occupancy < ss.min_ebs_occupancy) {
      ss.min_ebs_occupancy = node_occupancy;
    }

    count += 1;
  }

  ss.avg_ebs_occupancy = sum_ebs_occupancy / count;
  logger->info("Max EBS node occupancy is {}.", to_string(ss.max_ebs_occupancy));
  logger->info("Min EBS node occupancy is {}.", to_string(ss.min_ebs_occupancy));
  logger->info("Average EBS node occupancy is {}.", to_string(ss.avg_ebs_occupancy));
}

void collect_external_stats(
  unordered_map<address_t, double>& user_latency,
  unordered_map<address_t, double>& user_throughput,
  SummaryStats& ss,
  shared_ptr<spdlog::logger> logger) {
  // gather latency info
  if (user_latency.size() > 0) {
    // compute latency from users
    double sum_latency = 0;
    unsigned count = 0;

    for (auto it = user_latency.begin(); it != user_latency.end(); it++) {
      sum_latency += it->second;
      count += 1;
    }

    ss.avg_latency = sum_latency / count;
  }

  logger->info("Average latency is {}.", ss.avg_latency);

  // gather throughput info
  if (user_throughput.size() > 0) {
    // compute latency from users
    for (auto it = user_throughput.begin(); it != user_throughput.end(); it++) {
      ss.total_throughput += it->second;
    }
  }

  logger->info("Total throughput is {}.", ss.total_throughput);
}

KeyInfo create_new_replication_vector(unsigned gm, unsigned ge, unsigned lm, unsigned le) {
  KeyInfo rep_vector;
  rep_vector.global_replication_map_[1] = gm;
  rep_vector.global_replication_map_[2] = ge;
  rep_vector.local_replication_map_[1] = lm;
  rep_vector.local_replication_map_[2] = le;

  return rep_vector;
}

void prepare_replication_factor_update(
    string& key,
    unordered_map<address_t, communication::Replication_Factor_Request>& replication_factor_map,
    string server_address,
    unordered_map<string, KeyInfo>& placement) {

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
    unordered_map<string, KeyInfo>& requests,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    vector<address_t>& routing_address,
    unordered_map<string, KeyInfo>& placement,
    SocketCache& pushers,
    MonitoringThread& mt,
    zmq::socket_t& response_puller,
    shared_ptr<spdlog::logger> logger,
    unsigned& rid) {

  // used to keep track of the original replication factors for the requested keys
  unordered_map<string, KeyInfo> orig_placement_info;

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
          logger->info("Replication factor put for key {} rejected due to incorrect address.", res.tuple(i).key());

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
      for (unsigned tier = MIN_TIER; tier <= MAX_TIER; tier++) {
        unsigned rep = max(placement[key].global_replication_map_[tier], orig_placement_info[key].global_replication_map_[tier]);
        auto threads = responsible_global(key, rep, global_hash_ring_map[tier]);

        for (auto server_iter = threads.begin(); server_iter != threads.end(); server_iter++) {
          prepare_replication_factor_update(key, replication_factor_map, server_iter->get_replication_factor_change_connect_addr(), placement);
        }
      }

      // form placement requests for routing nodes
      for (auto routing_iter = routing_address.begin(); routing_iter != routing_address.end(); routing_iter++) {
        prepare_replication_factor_update(key, replication_factor_map, RoutingThread(*routing_iter, 0).get_replication_factor_change_connect_addr(), placement);
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
  auto logger = spdlog::basic_logger_mt("monitoring_logger", "log.txt", true);
  logger->flush_on(spdlog::level::info);

  if (argc != 1) {
    cerr << "Usage: " << argv[0] << endl;
    return 1;
  }

  YAML::Node conf = YAML::LoadFile("conf/config.yml")["monitoring"];
  string ip = conf["ip"].as<string>();

  tier_data_map[1] = TierData(MEMORY_THREAD_NUM, DEFAULT_GLOBAL_MEMORY_REPLICATION, MEM_NODE_CAPACITY);
  tier_data_map[2] = TierData(EBS_THREAD_NUM, DEFAULT_GLOBAL_EBS_REPLICATION, EBS_NODE_CAPACITY);

  // initialize hash ring maps
  unordered_map<unsigned, GlobalHashRing> global_hash_ring_map;
  unordered_map<unsigned, LocalHashRing> local_hash_ring_map;

  // form local hash rings
  for (auto it = tier_data_map.begin(); it != tier_data_map.end(); it++) {
    for (unsigned tid = 0; tid < it->second.thread_number_; tid++) {
      insert_to_hash_ring<LocalHashRing>(local_hash_ring_map[it->first], ip, tid);
    }
  }

  // keep track of the keys' replication info
  unordered_map<string, KeyInfo> placement;
  // warm up for benchmark
  warmup_placement_to_defaults(placement);

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

  // keep track of some summary statistics
  SummaryStats ss;

  // keep track of user latency info
  unordered_map<address_t, double> user_latency;

  // keep track of user throughput info
  unordered_map<address_t, double> user_throughput;

  // used for adjusting the replication factors based on feedback from the user
  unordered_map<string, pair<double, unsigned>> rep_factor_map;

  vector<address_t> routing_address;

  // read the YAML conf
  address_t management_address = conf["mgmt_ip"].as<string>();
  MonitoringThread mt = MonitoringThread(ip);

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  // responsible for listening to the response of the replication factor change request
  zmq::socket_t response_puller(context, ZMQ_PULL);
  int timeout = 10000;

  response_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  response_puller.bind(mt.get_request_pulling_bind_addr());

  // keep track of departing node status
  unordered_map<address_t, unsigned> departing_node_map;

  // responsible for both node join and departure
  zmq::socket_t notify_puller(context, ZMQ_PULL);
  notify_puller.bind(mt.get_notify_bind_addr());

  // responsible for receiving depart done notice
  zmq::socket_t depart_done_puller(context, ZMQ_PULL);
  depart_done_puller.bind(mt.get_depart_done_bind_addr());

  // responsible for receiving feedback from users
  zmq::socket_t feedback_puller(context, ZMQ_PULL);
  feedback_puller.bind(mt.get_latency_report_bind_addr());

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(notify_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(depart_done_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(feedback_puller), 0, ZMQ_POLLIN, 0 }
  };

  auto report_start = chrono::system_clock::now();
  auto report_end = chrono::system_clock::now();

  auto grace_start = chrono::system_clock::now();

  unsigned adding_memory_node = 0;
  unsigned adding_ebs_node = 0;
  bool removing_memory_node = false;
  bool removing_ebs_node = false;

  unsigned server_monitoring_epoch = 0;

  unsigned rid = 0;

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
        logger->info("Received join from server {} in tier {}.", new_server_ip, to_string(tier));
        if (tier == 1) {
          insert_to_hash_ring<GlobalHashRing>(global_hash_ring_map[tier], new_server_ip, 0);

          if (adding_memory_node > 0) {
            adding_memory_node -= 1;
          }

          // reset grace period timer
          grace_start = chrono::system_clock::now();
        } else if (tier == 2) {
          insert_to_hash_ring<GlobalHashRing>(global_hash_ring_map[tier], new_server_ip, 0);

          if (adding_ebs_node > 0) {
            adding_ebs_node -= 1;
          }

          // reset grace period timer
          grace_start = chrono::system_clock::now();
        } else if (tier == 0) {
          routing_address.push_back(new_server_ip);
        } else {
          logger->error("Invalid tier: {}.", to_string(tier));
        }

        for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
          logger->info("Hash ring for tier {} is size {}.", to_string(it->first), to_string(it->second.size()));
        }
      } else if (type == "depart") {
        logger->info("Received depart from server {}.", new_server_ip);

        // update hash ring
        if (tier == 1) {
          remove_from_hash_ring<GlobalHashRing>(global_hash_ring_map[tier], new_server_ip, 0);
          memory_tier_storage.erase(new_server_ip);
          memory_tier_occupancy.erase(new_server_ip);

          for (auto it = key_access_frequency.begin(); it != key_access_frequency.end(); it++) {
            for (unsigned i = 0; i < MEMORY_THREAD_NUM; i++) {
              it->second.erase(new_server_ip + ":" + to_string(i));
            }
          }
        } else if (tier == 2) {
          remove_from_hash_ring<GlobalHashRing>(global_hash_ring_map[tier], new_server_ip, 0);
          ebs_tier_storage.erase(new_server_ip);
          ebs_tier_occupancy.erase(new_server_ip);

          for (auto it = key_access_frequency.begin(); it != key_access_frequency.end(); it++) {
            for (unsigned i = 0; i < EBS_THREAD_NUM; i++) {
              it->second.erase(new_server_ip + ":" + to_string(i));
            }
          }
        } else {
          logger->error("Invalid tier: {}.", to_string(tier));
        }

        for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
          logger->info("Hash ring for tier {} is size {}.", to_string(it->first), to_string(it->second.size()));
        }
      }
    }

    // handle a depart done notification
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
            logger->info("Removing memory node {}.", departed_ip);

            string shell_command = "curl -X POST http://" + management_address + "/remove/memory/" + departed_ip;
            system(shell_command.c_str());

            removing_memory_node = false;
          } else {
            logger->info("Removing ebs node {}", departed_ip);

            string shell_command = "curl -X POST http://" + management_address + "/remove/ebs/" + departed_ip;
            system(shell_command.c_str());

            removing_ebs_node = false;
          }

          // reset grace period timer
          grace_start = chrono::system_clock::now();
          departing_node_map.erase(departed_ip);
        }
      } else {
        logger->error("Missing entry in the depart done map.");
      }
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      string serialized_feedback = zmq_util::recv_string(&feedback_puller);
      communication::Feedback fb;
      fb.ParseFromString(serialized_feedback);

      if (fb.has_finish() && fb.finish()) {
        user_latency.erase(fb.uid());
      } else {
        // collect latency and throughput feedback
        user_latency[fb.uid()] = fb.latency();
        user_throughput[fb.uid()] = fb.throughput();

        // collect replication factor adjustment factors
        for (int i = 0; i < fb.rep_size(); i++) {
          string key = fb.rep(i).key();
          double factor = fb.rep(i).factor();

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

      ss.clear();

      user_latency.clear();
      user_throughput.clear();
      rep_factor_map.clear();

      // collect internal statistics
      collect_internal_stats(global_hash_ring_map,
          local_hash_ring_map,
          pushers,
          mt,
          response_puller,
          logger,
          rid,
          key_access_frequency,
          memory_tier_storage,
          ebs_tier_storage,
          memory_tier_occupancy,
          ebs_tier_occupancy,
          memory_tier_access,
          ebs_tier_access);

      // compute summary statistics
      compute_summary_stats(key_access_frequency,
          memory_tier_storage,
          ebs_tier_storage,
          memory_tier_occupancy,
          ebs_tier_occupancy,
          memory_tier_access,
          ebs_tier_access,
          key_access_summary,
          ss,
          logger,
          server_monitoring_epoch);

      // collect external statistics
      collect_external_stats(user_latency, user_throughput, ss, logger);

      unsigned memory_node_number = global_hash_ring_map[1].size() / VIRTUAL_THREAD_NUM;
      unsigned ebs_node_number = global_hash_ring_map[2].size() / VIRTUAL_THREAD_NUM;

      // Policy Start Here:
      unordered_map<string, KeyInfo> requests;
      unsigned total_rep_to_change = 0;

      // 1. first check storage consumption and trigger elasticity if necessary
      if (adding_memory_node == 0 && ss.required_memory_node > memory_node_number) {
        auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-grace_start).count();

        if (time_elapsed > GRACE_PERIOD) {
          logger->info("Adding {} memory nodes.", to_string(NODE_ADD));

          string shell_command = "curl -X POST http://" + management_address + "/add/memory/" + to_string(NODE_ADD) + " &";
          system(shell_command.c_str());
          adding_memory_node = NODE_ADD;
        }
      }

      if (adding_ebs_node == 0 && ss.required_ebs_node > ebs_node_number) {
        auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-grace_start).count();

        if (time_elapsed > GRACE_PERIOD) {
          logger->info("Adding {} EBS node.", to_string(NODE_ADD));

          string shell_command = "curl -X POST http://" + management_address + "/add/ebs/" + to_string(NODE_ADD) + " &";
          system(shell_command.c_str());
          adding_ebs_node = NODE_ADD;
        }
      }

      if (ss.avg_ebs_consumption_percentage < EBS_CAPACITY_MIN && !removing_ebs_node && ebs_node_number > max(ss.required_ebs_node, (unsigned)MINIMUM_EBS_NODE)) {
        auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-grace_start).count();

        if (time_elapsed > GRACE_PERIOD) {
          // pick a random ebs node and send remove node command
          auto node = next(begin(global_hash_ring_map[2]), rand() % global_hash_ring_map[2].size())->second;
          auto ip = node.get_ip();
          auto connection_addr = node.get_self_depart_connect_addr();

          logger->info("Triggering removal of EBS node {}.", ip);

          departing_node_map[ip] = tier_data_map[2].thread_number_;
          auto ack_addr = mt.get_depart_done_connect_addr();
          zmq_util::send_string(ack_addr, &pushers[connection_addr]);
          removing_ebs_node = true;
        }
      }

      // 2. check key access summary to promote hot keys to memory tier
      unsigned slot = (MEM_CAPACITY_MAX * tier_data_map[1].node_capacity_ * memory_node_number - ss.total_memory_consumption) / VALUE_SIZE;
      bool overflow = false;

      for (auto it = key_access_summary.begin(); it != key_access_summary.end(); it++) {
        string key = it->first;
        unsigned total_access = it->second;

        if (!is_metadata(key) && total_access > PROMOTE_THRESHOLD && placement[key].global_replication_map_[1] == 0) {
          total_rep_to_change += 1;

          if (total_rep_to_change > slot) {
            overflow = true;
          } else {
            requests[key] = create_new_replication_vector(placement[key].global_replication_map_[1] + 1, placement[key].global_replication_map_[2] - 1, placement[key].local_replication_map_[1], placement[key].local_replication_map_[2]);
          }
        }
      }

      change_replication_factor(requests, global_hash_ring_map, local_hash_ring_map, routing_address, placement, pushers, mt, response_puller, logger, rid);
      logger->info("Promoting {} keys into {} memory slots.", total_rep_to_change, slot);
      auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-grace_start).count();

      if (overflow && adding_memory_node == 0 && time_elapsed > GRACE_PERIOD) {
        unsigned long long promote_data_size = total_rep_to_change * VALUE_SIZE;
        unsigned total_memory_node_needed = ceil((ss.total_memory_consumption + promote_data_size) / (MEM_CAPACITY_MAX * tier_data_map[1].node_capacity_));

        if (total_memory_node_needed > memory_node_number) {
          unsigned node_to_add = (total_memory_node_needed - memory_node_number);
          logger->info("Insufficient memory nodes: Adding {} memory nodes.", to_string(node_to_add));

          string shell_command = "curl -X POST http://" + management_address + "/add/memory/" + to_string(node_to_add) + " &";
          system(shell_command.c_str());
          adding_memory_node = node_to_add;
        }
      }

      requests.clear();
      total_rep_to_change = 0;

      // 3. check key access summary to demote cold keys to ebs tier
      time_elapsed = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-grace_start).count();

      if (time_elapsed > GRACE_PERIOD) {
        unsigned slot = (EBS_CAPACITY_MAX * tier_data_map[2].node_capacity_ * ebs_node_number - ss.total_ebs_consumption) / VALUE_SIZE;
        bool overflow = false;

        for (auto it = key_access_summary.begin(); it != key_access_summary.end(); it++) {
          string key = it->first;
          unsigned total_access = it->second;

          if (!is_metadata(key) && total_access < DEMOTE_THRESHOLD && placement[key].global_replication_map_[1] > 0) {
            total_rep_to_change += 1;

            if (total_rep_to_change > slot) {
              overflow = true;
            } else {
              requests[key] = create_new_replication_vector(0, MINIMUM_REPLICA_NUMBER, 1, 1);
            }
          }
        }

        change_replication_factor(requests, global_hash_ring_map, local_hash_ring_map, routing_address, placement, pushers, mt, response_puller, logger, rid);
        logger->info("Demoting {} keys into {} EBS slots.", total_rep_to_change, slot);
        if (overflow && adding_ebs_node == 0) {
          unsigned long long demote_data_size = total_rep_to_change * VALUE_SIZE;
          unsigned total_ebs_node_needed = ceil((ss.total_ebs_consumption + demote_data_size) / (EBS_CAPACITY_MAX * tier_data_map[2].node_capacity_));

          if (total_ebs_node_needed > ebs_node_number) {
            unsigned node_to_add = (total_ebs_node_needed - ebs_node_number);
            logger->info("Insufficient EBS nodes; Adding {} EBS nodes.", to_string(node_to_add));

            string shell_command = "curl -X POST http://" + management_address + "/add/ebs/" + to_string(node_to_add) + " &";
            system(shell_command.c_str());
            adding_ebs_node = node_to_add;
          }
        }
      }

      requests.clear();
      total_rep_to_change = 0;

      // 4.1 if latency is too high
      if (ss.avg_latency > SLO_WORST && adding_memory_node == 0) {
        logger->info("Observed latency ({}) violates SLO({}).", ss.avg_latency, SLO_WORST);

        // figure out if we should do hot key replication or add nodes
        if (ss.min_memory_occupancy > 0.15) {
          unsigned node_to_add = ceil((ss.avg_latency / SLO_WORST - 1) * memory_node_number);

          // trigger elasticity
          auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-grace_start).count();
          if (time_elapsed > GRACE_PERIOD) {
            logger->info("Adding {} memory nodes.", to_string(node_to_add));
            string shell_command = "curl -X POST http://" + management_address + "/add/memory/" + to_string(node_to_add) + " &";
            system(shell_command.c_str());
            adding_memory_node = node_to_add;
          }
        } else { // hot key replication
          // find hot keys
          logger->info("Classifying hot keys...");
          for (auto it = key_access_summary.begin(); it != key_access_summary.end(); it++) {
            string key = it->first;
            unsigned total_access = it->second;

            if (!is_metadata(key) && total_access > ss.key_access_mean + ss.key_access_std && rep_factor_map.find(key) != rep_factor_map.end()) {
              logger->info("Key {} accessed {} times (threshold is {}).", key, total_access, ss.key_access_mean + ss.key_access_std);
              unsigned target_rep_factor = placement[key].global_replication_map_[1] * rep_factor_map[key].first;

              if (target_rep_factor == placement[key].global_replication_map_[1]) {
                target_rep_factor += 1;
              }

              unsigned current_mem_rep = placement[key].global_replication_map_[1];
              if (target_rep_factor > current_mem_rep && current_mem_rep < memory_node_number) {
                unsigned new_mem_rep = min(memory_node_number, target_rep_factor);
                unsigned new_ebs_rep = max(MINIMUM_REPLICA_NUMBER - new_mem_rep, (unsigned) 0);
                requests[key] = create_new_replication_vector(new_mem_rep, new_ebs_rep, placement[key].local_replication_map_[1], placement[key].local_replication_map_[2]);
                logger->info("Global hot key replication for key {}. M: {}->{}.", key, placement[key].global_replication_map_[1], requests[key].global_replication_map_[1]);
              } else {
                if (MEMORY_THREAD_NUM > placement[key].local_replication_map_[1]) {
                  requests[key] = create_new_replication_vector(placement[key].global_replication_map_[1], placement[key].global_replication_map_[2], MEMORY_THREAD_NUM, placement[key].local_replication_map_[2]);
                  logger->info("Local hot key replication for key {}. T: {}->{}.", key, placement[key].local_replication_map_[1], requests[key].local_replication_map_[1]);
                }
              }
            }
          }

          change_replication_factor(requests, global_hash_ring_map, local_hash_ring_map, routing_address, placement, pushers, mt, response_puller, logger, rid);
        }
      }

      requests.clear();

      // 4.3 if latency is fine, check if there is underutilized memory node
      if (ss.min_memory_occupancy < 0.05 && !removing_memory_node && memory_node_number > max(ss.required_memory_node, (unsigned)MINIMUM_MEMORY_NODE)) {
        logger->info("Node {} is severely underutilized.", ss.min_occupancy_memory_ip);
        auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-grace_start).count();

        if (time_elapsed > GRACE_PERIOD) {
          // before sending remove command, first adjust relevant key's replication factor
          for (auto it = key_access_summary.begin(); it != key_access_summary.end(); it++) {
            string key = it->first;

            if (!is_metadata(key) && placement[key].global_replication_map_[1] == (global_hash_ring_map[1].size() / VIRTUAL_THREAD_NUM)) {
              unsigned new_mem_rep = placement[key].global_replication_map_[1] - 1;
              unsigned new_ebs_rep = max(MINIMUM_REPLICA_NUMBER - new_mem_rep, (unsigned) 0);
              requests[key] = create_new_replication_vector(new_mem_rep, new_ebs_rep, placement[key].local_replication_map_[1], placement[key].local_replication_map_[2]);
              logger->info("Dereplication for key {}. M: {}->{}. E: {}->{}", key, placement[key].global_replication_map_[1], requests[key].global_replication_map_[1], placement[key].global_replication_map_[2], requests[key].global_replication_map_[2]);
            }
          }

          change_replication_factor(requests, global_hash_ring_map, local_hash_ring_map, routing_address, placement, pushers, mt, response_puller, logger, rid);

          ServerThread node = ServerThread(ss.min_occupancy_memory_ip, 0);
          auto connection_addr = node.get_self_depart_connect_addr();

          departing_node_map[ss.min_occupancy_memory_ip] = tier_data_map[1].thread_number_;
          auto ack_addr = mt.get_depart_done_connect_addr();

          logger->info("Removing memory node {}.", node.get_ip());
          zmq_util::send_string(ack_addr, &pushers[connection_addr]);
          removing_memory_node = true;
        }
      }

      requests.clear();

      // finally, consider reducing the replication factor of some keys that are not so hot anymore
      for (auto it = key_access_summary.begin(); it != key_access_summary.end(); it++) {
        string key = it->first;
        unsigned total_access = it->second;

        if (!is_metadata(key) && total_access <= ss.key_access_mean && placement[key].global_replication_map_[1] > MINIMUM_REPLICA_NUMBER) {
          logger->info("Key {} accessed {} times (threshold is {}).", key, total_access, ss.key_access_mean);
          requests[key] = create_new_replication_vector(1, MINIMUM_REPLICA_NUMBER - 1, 1, 1);
          logger->info("Dereplication for key {}. M: {}->{}. E: {}->{}", key, placement[key].global_replication_map_[1], requests[key].global_replication_map_[1], placement[key].global_replication_map_[2], requests[key].global_replication_map_[2]);
        }
      }

      change_replication_factor(requests, global_hash_ring_map, local_hash_ring_map, routing_address, placement, pushers, mt, response_puller, logger, rid);
      requests.clear();

      logger->info("Adding {} memory nodes is in progress.", adding_memory_node);
      logger->info("Adding {} EBS nodes in progress.", adding_ebs_node);

      report_start = std::chrono::system_clock::now();
    }
  }
}
