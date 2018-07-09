#include "hash_ring.hpp"
#include "monitor/monitoring_utils.hpp"
#include "requests.hpp"
#include "spdlog/spdlog.h"

using namespace std;

void collect_internal_stats(
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    SocketCache& pushers, MonitoringThread& mt, zmq::socket_t& response_puller,
    shared_ptr<spdlog::logger> logger, unsigned& rid,
    unordered_map<string, unordered_map<string, unsigned>>&
        key_access_frequency,
    unordered_map<string, unordered_map<unsigned, unsigned long long>>&
        memory_tier_storage,
    unordered_map<string, unordered_map<unsigned, unsigned long long>>&
        ebs_tier_storage,
    unordered_map<string, unordered_map<unsigned, pair<double, unsigned>>>&
        memory_tier_occupancy,
    unordered_map<string, unordered_map<unsigned, pair<double, unsigned>>>&
        ebs_tier_occupancy,
    unordered_map<string, unordered_map<unsigned, unsigned>>&
        memory_tier_access,
    unordered_map<string, unordered_map<unsigned, unsigned>>& ebs_tier_access,
    unordered_map<unsigned, TierData>& tier_data_map) {
  unordered_map<string, communication::Request> addr_request_map;

  for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end();
       it++) {
    unsigned tier_id = it->first;
    auto hash_ring = &(it->second);
    unordered_set<string> observed_ip;

    for (auto iter = hash_ring->begin(); iter != hash_ring->end(); iter++) {
      if (observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
        for (unsigned i = 0; i < tier_data_map[tier_id].thread_number_; i++) {
          string key = string(METADATA_IDENTIFIER) + "_" +
                       iter->second.get_ip() + "_" + to_string(i) + "_" +
                       to_string(tier_id) + "_stat";
          prepare_metadata_get_request(key, global_hash_ring_map[1],
                                       local_hash_ring_map[1], addr_request_map,
                                       mt, rid);

          key = string(METADATA_IDENTIFIER) + "_" + iter->second.get_ip() +
                "_" + to_string(i) + "_" + to_string(tier_id) + "_access";
          prepare_metadata_get_request(key, global_hash_ring_map[1],
                                       local_hash_ring_map[1], addr_request_map,
                                       mt, rid);
        }
        observed_ip.insert(iter->second.get_ip());
      }
    }
  }

  for (auto it = addr_request_map.begin(); it != addr_request_map.end(); it++) {
    bool succeed;
    auto res = send_request<communication::Request, communication::Response>(
        it->second, pushers[it->first], response_puller, succeed);

    if (succeed) {
      for (int i = 0; i < res.tuple_size(); i++) {
        if (res.tuple(i).err_number() == 0) {
          vector<string> tokens;
          split(res.tuple(i).key(), '_', tokens);
          string ip = tokens[1];

          unsigned tid = stoi(tokens[2]);
          unsigned tier_id = stoi(tokens[3]);
          string metadata_type = tokens[4];

          if (metadata_type == "stat") {
            // deserialized the value
            communication::Server_Stat stat;
            stat.ParseFromString(res.tuple(i).value());

            if (tier_id == 1) {
              memory_tier_storage[ip][tid] = stat.storage_consumption();
              memory_tier_occupancy[ip][tid] =
                  pair<double, unsigned>(stat.occupancy(), stat.epoch());
              memory_tier_access[ip][tid] = stat.total_access();
            } else {
              ebs_tier_storage[ip][tid] = stat.storage_consumption();
              ebs_tier_occupancy[ip][tid] =
                  pair<double, unsigned>(stat.occupancy(), stat.epoch());
              ebs_tier_access[ip][tid] = stat.total_access();
            }
          } else if (metadata_type == "access") {
            // deserialized the value
            communication::Key_Access access;
            access.ParseFromString(res.tuple(i).value());

            if (tier_id == 1) {
              for (int j = 0; j < access.tuple_size(); j++) {
                string key = access.tuple(j).key();
                key_access_frequency[key][ip + ":" + to_string(tid)] =
                    access.tuple(j).access();
              }
            } else {
              for (int j = 0; j < access.tuple_size(); j++) {
                string key = access.tuple(j).key();
                key_access_frequency[key][ip + ":" + to_string(tid)] =
                    access.tuple(j).access();
              }
            }
          }
        } else if (res.tuple(i).err_number() == 1) {
          logger->error("Key {} doesn't exist.", res.tuple(i).key());
        } else {
          // The hash ring should never be inconsistent.
          logger->error("Hash ring is inconsistent for key {}.",
                        res.tuple(i).key());
        }
      }
    } else {
      logger->error("Request timed out.");
      continue;
    }
  }
}

void compute_summary_stats(
    unordered_map<string, unordered_map<string, unsigned>>&
        key_access_frequency,
    unordered_map<string, unordered_map<unsigned, unsigned long long>>&
        memory_tier_storage,
    unordered_map<string, unordered_map<unsigned, unsigned long long>>&
        ebs_tier_storage,
    unordered_map<string, unordered_map<unsigned, pair<double, unsigned>>>&
        memory_tier_occupancy,
    unordered_map<string, unordered_map<unsigned, pair<double, unsigned>>>&
        ebs_tier_occupancy,
    unordered_map<string, unordered_map<unsigned, unsigned>>&
        memory_tier_access,
    unordered_map<string, unordered_map<unsigned, unsigned>>& ebs_tier_access,
    unordered_map<string, unsigned>& key_access_summary, SummaryStats& ss,
    shared_ptr<spdlog::logger> logger, unsigned& server_monitoring_epoch,
    unordered_map<unsigned, TierData>& tier_data_map) {
  // compute key access summary
  unsigned cnt = 0;
  double mean = 0;
  double ms = 0;

  for (auto it = key_access_frequency.begin(); it != key_access_frequency.end();
       it++) {
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

  logger->info("Access: mean={}, std={}", ss.key_access_mean,
               ss.key_access_std);

  // compute tier access summary
  for (auto it = memory_tier_access.begin(); it != memory_tier_access.end();
       it++) {
    for (auto iter = it->second.begin(); iter != it->second.end(); iter++) {
      ss.total_memory_access += iter->second;
    }
  }

  for (auto it = ebs_tier_access.begin(); it != ebs_tier_access.end(); it++) {
    for (auto iter = it->second.begin(); iter != it->second.end(); iter++) {
      ss.total_ebs_access += iter->second;
    }
  }

  logger->info("Total accesses: memory={}, ebs={}", ss.total_memory_access,
               ss.total_ebs_access);

  // compute storage consumption related statistics
  unsigned m_count = 0;
  unsigned e_count = 0;

  for (auto it1 = memory_tier_storage.begin(); it1 != memory_tier_storage.end();
       it1++) {
    unsigned total_thread_consumption = 0;

    for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
      ss.total_memory_consumption += it2->second;
      total_thread_consumption += it2->second;
    }

    double percentage = (double)total_thread_consumption /
                        (double)tier_data_map[1].node_capacity_;
    logger->info("Memory node {} storage consumption is {}.", it1->first,
                 percentage);

    if (percentage > ss.max_memory_consumption_percentage) {
      ss.max_memory_consumption_percentage = percentage;
    }

    m_count += 1;
  }

  for (auto it1 = ebs_tier_storage.begin(); it1 != ebs_tier_storage.end();
       it1++) {
    unsigned total_thread_consumption = 0;

    for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
      ss.total_ebs_consumption += it2->second;
      total_thread_consumption += it2->second;
    }

    double percentage = (double)total_thread_consumption /
                        (double)tier_data_map[2].node_capacity_;
    logger->info("EBS node {} storage consumption is {}.", it1->first,
                 percentage);

    if (percentage > ss.max_ebs_consumption_percentage) {
      ss.max_ebs_consumption_percentage = percentage;
    }
    e_count += 1;
  }

  if (m_count != 0) {
    ss.avg_memory_consumption_percentage =
        (double)ss.total_memory_consumption /
        ((double)m_count * tier_data_map[1].node_capacity_);
    logger->info("Average memory node consumption is {}.",
                 ss.avg_memory_consumption_percentage);
    logger->info("Max memory node consumption is {}.",
                 ss.max_memory_consumption_percentage);
  }

  if (e_count != 0) {
    ss.avg_ebs_consumption_percentage =
        (double)ss.total_ebs_consumption /
        ((double)e_count * tier_data_map[2].node_capacity_);
    logger->info("Average EBS node consumption is {}.",
                 ss.avg_ebs_consumption_percentage);
    logger->info("Max EBS node consumption is {}.",
                 ss.max_ebs_consumption_percentage);
  }

  ss.required_memory_node =
      ceil(ss.total_memory_consumption /
           (MEM_CAPACITY_MAX * tier_data_map[1].node_capacity_));
  ss.required_ebs_node =
      ceil(ss.total_ebs_consumption /
           (EBS_CAPACITY_MAX * tier_data_map[2].node_capacity_));

  logger->info("The system requires {} new memory nodes.",
               ss.required_memory_node);
  logger->info("The system requires {} new EBS nodes.", ss.required_ebs_node);

  // compute occupancy related statistics
  double sum_memory_occupancy = 0.0;

  unsigned count = 0;

  for (auto it1 = memory_tier_occupancy.begin();
       it1 != memory_tier_occupancy.end(); it1++) {
    double sum_thread_occupancy = 0.0;
    unsigned thread_count = 0;

    for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
      logger->info(
          "Memory node {} thread {} occupancy is {} at epoch {} (monitoring "
          "epoch {}).",
          it1->first, it2->first, it2->second.first, it2->second.second,
          server_monitoring_epoch);
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
  logger->info("Max memory node occupancy is {}.",
               to_string(ss.max_memory_occupancy));
  logger->info("Min memory node occupancy is {}.",
               to_string(ss.min_memory_occupancy));
  logger->info("Average memory node occupancy is {}.",
               to_string(ss.avg_memory_occupancy));

  double sum_ebs_occupancy = 0.0;

  count = 0;

  for (auto it1 = ebs_tier_occupancy.begin(); it1 != ebs_tier_occupancy.end();
       it1++) {
    double sum_thread_occupancy = 0.0;
    unsigned thread_count = 0;

    for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
      logger->info(
          "EBS node {} thread {} occupancy is {} at epoch {} (monitoring epoch "
          "{}).",
          it1->first, it2->first, it2->second.first, it2->second.second,
          server_monitoring_epoch);
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
  logger->info("Max EBS node occupancy is {}.",
               to_string(ss.max_ebs_occupancy));
  logger->info("Min EBS node occupancy is {}.",
               to_string(ss.min_ebs_occupancy));
  logger->info("Average EBS node occupancy is {}.",
               to_string(ss.avg_ebs_occupancy));
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