#include "spdlog/spdlog.h"
#include "hash_ring.hpp"
#include "requests.hpp"
#include "monitor/monitoring_utils.hpp"

using namespace std;

string prepare_metadata_request(string& key,
                                GlobalHashRing& global_memory_hash_ring,
                                LocalHashRing& local_memory_hash_ring,
                                unordered_map<string, communication::Request>& addr_request_map,
                                MonitoringThread& mt,
                                unsigned& rid,
                                string type) {
  auto threads = get_responsible_threads_metadata(key, global_memory_hash_ring, local_memory_hash_ring);
  if (threads.size() != 0) {
    string target_address = next(begin(threads), rand() % threads.size())->get_request_pulling_connect_addr();
    if (addr_request_map.find(target_address) == addr_request_map.end()) {
      addr_request_map[target_address].set_type(type);
      addr_request_map[target_address].set_respond_address(mt.get_request_pulling_connect_addr());
      string req_id = mt.get_ip() + ":" + to_string(rid);
      addr_request_map[target_address].set_request_id(req_id);
      rid += 1;
    }

    return target_address;
  }

  return string();
}

void prepare_metadata_get_request(string& key,
                                  GlobalHashRing& global_memory_hash_ring,
                                  LocalHashRing& local_memory_hash_ring,
                                  unordered_map<string, communication::Request>& addr_request_map,
                                  MonitoringThread& mt,
                                  unsigned& rid) {
  string target_address = prepare_metadata_request(key, global_memory_hash_ring, local_memory_hash_ring, addr_request_map, mt, rid, "GET");

  if (!target_address.empty()) {
    prepare_get_tuple(addr_request_map[target_address], key);
  }
}

void prepare_metadata_put_request(string& key,
                                  string& value,
                                  GlobalHashRing& global_memory_hash_ring,
                                  LocalHashRing& local_memory_hash_ring,
                                  unordered_map<string, communication::Request>& addr_request_map,
                                  MonitoringThread& mt,
                                  unsigned& rid) {
  string target_address = prepare_metadata_request(key, global_memory_hash_ring, local_memory_hash_ring, addr_request_map, mt, rid, "PUT");

  if (!target_address.empty()) {
    prepare_put_tuple(addr_request_map[target_address], key, value, 0);
  }
}

void collect_internal_stats(unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
                            unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
                            SocketCache& pushers,
                            MonitoringThread& mt,
                            zmq::socket_t& response_puller,
                            shared_ptr<spdlog::logger> logger,
                            unsigned& rid,
                            unordered_map<string, unordered_map<string, unsigned>>& key_access_frequency,
                            unordered_map<string, unordered_map<unsigned, unsigned long long>>& memory_tier_storage,
                            unordered_map<string, unordered_map<unsigned, unsigned long long>>& ebs_tier_storage,
                            unordered_map<string, unordered_map<unsigned, pair<double, unsigned>>>& memory_tier_occupancy,
                            unordered_map<string, unordered_map<unsigned, pair<double, unsigned>>>& ebs_tier_occupancy,
                            unordered_map<string, unordered_map<unsigned, unsigned>>& memory_tier_access,
                            unordered_map<string, unordered_map<unsigned, unsigned>>& ebs_tier_access,
                            unordered_map<unsigned, TierData>& tier_data_map) {
  unordered_map<string, communication::Request> addr_request_map;

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

void compute_summary_stats(unordered_map<string, unordered_map<string, unsigned>>& key_access_frequency,
                           unordered_map<string, unordered_map<unsigned, unsigned long long>>& memory_tier_storage,
                           unordered_map<string, unordered_map<unsigned, unsigned long long>>& ebs_tier_storage,
                           unordered_map<string, unordered_map<unsigned, pair<double, unsigned>>>& memory_tier_occupancy,
                           unordered_map<string, unordered_map<unsigned, pair<double, unsigned>>>& ebs_tier_occupancy,
                           unordered_map<string, unordered_map<unsigned, unsigned>>& memory_tier_access,
                           unordered_map<string, unordered_map<unsigned, unsigned>>& ebs_tier_access,
                           unordered_map<string, unsigned>& key_access_summary,
                           SummaryStats& ss,
                           shared_ptr<spdlog::logger> logger,
                           unsigned& server_monitoring_epoch,
                           unordered_map<unsigned, TierData>& tier_data_map,
                           const double MEM_CAPACITY_MAX,
                           const double EBS_CAPACITY_MAX) {
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

void collect_external_stats(unordered_map<string, double>& user_latency,
                            unordered_map<string, double>& user_throughput,
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

void prepare_replication_factor_update(string& key,
                                       unordered_map<string, communication::Replication_Factor_Request>& replication_factor_map,
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
void change_replication_factor(unordered_map<string, KeyInfo>& requests,
                               unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
                               unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
                               vector<string>& routing_address,
                               unordered_map<string, KeyInfo>& placement,
                               SocketCache& pushers,
                               MonitoringThread& mt,
                               zmq::socket_t& response_puller,
                               shared_ptr<spdlog::logger> logger,
                               unsigned& rid) {
  // used to keep track of the original replication factors for the requested keys
  unordered_map<string, KeyInfo> orig_placement_info;

  // store the new replication factor synchronously in storage servers
  unordered_map<string, communication::Request> addr_request_map;

  // form the placement request map
  unordered_map<string, communication::Replication_Factor_Request> replication_factor_map;

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