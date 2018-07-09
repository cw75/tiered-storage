#include "hash_ring.hpp"
#include "monitor/monitoring_utils.hpp"
#include "spdlog/spdlog.h"

void movement_policy(
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::chrono::time_point<std::chrono::system_clock>& grace_start,
    SummaryStats& ss, unsigned& memory_node_number, unsigned& ebs_node_number,
    unsigned& adding_memory_node, unsigned& adding_ebs_node,
    std::string management_address,
    std::unordered_map<std::string, KeyInfo>& placement,
    std::unordered_map<std::string, unsigned>& key_access_summary,
    MonitoringThread& mt, std::unordered_map<unsigned, TierData>& tier_data_map,
    SocketCache& pushers, zmq::socket_t& response_puller,
    std::vector<std::string>& routing_address, unsigned& rid) {
  // promote hot keys to memory tier
  std::unordered_map<std::string, KeyInfo> requests;
  unsigned total_rep_to_change = 0;
  unsigned slot =
      (MEM_CAPACITY_MAX * tier_data_map[1].node_capacity_ * memory_node_number -
       ss.total_memory_consumption) /
      VALUE_SIZE;
  bool overflow = false;

  for (auto it = key_access_summary.begin(); it != key_access_summary.end();
       it++) {
    std::string key = it->first;
    unsigned total_access = it->second;

    if (!is_metadata(key) && total_access > PROMOTE_THRESHOLD &&
        placement[key].global_replication_map_[1] == 0) {
      total_rep_to_change += 1;

      if (total_rep_to_change > slot) {
        overflow = true;
      } else {
        requests[key] = create_new_replication_vector(
            placement[key].global_replication_map_[1] + 1,
            placement[key].global_replication_map_[2] - 1,
            placement[key].local_replication_map_[1],
            placement[key].local_replication_map_[2]);
      }
    }
  }

  change_replication_factor(requests, global_hash_ring_map, local_hash_ring_map,
                            routing_address, placement, pushers, mt,
                            response_puller, logger, rid);
  logger->info("Promoting {} keys into {} memory slots.", total_rep_to_change,
               slot);
  auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                          std::chrono::system_clock::now() - grace_start)
                          .count();

  if (overflow && adding_memory_node == 0 && time_elapsed > GRACE_PERIOD) {
    unsigned long long promote_data_size = total_rep_to_change * VALUE_SIZE;
    unsigned total_memory_node_needed =
        ceil((ss.total_memory_consumption + promote_data_size) /
             (MEM_CAPACITY_MAX * tier_data_map[1].node_capacity_));

    if (total_memory_node_needed > memory_node_number) {
      unsigned node_to_add = (total_memory_node_needed - memory_node_number);
      add_node(logger, "memory", node_to_add, adding_memory_node,
               management_address);
    }
  }

  requests.clear();
  total_rep_to_change = 0;

  // demote cold keys to ebs tier
  slot = (EBS_CAPACITY_MAX * tier_data_map[2].node_capacity_ * ebs_node_number -
          ss.total_ebs_consumption) /
         VALUE_SIZE;
  overflow = false;

  for (auto it = key_access_summary.begin(); it != key_access_summary.end();
       it++) {
    std::string key = it->first;
    unsigned total_access = it->second;

    if (!is_metadata(key) && total_access < DEMOTE_THRESHOLD &&
        placement[key].global_replication_map_[1] > 0) {
      total_rep_to_change += 1;

      if (total_rep_to_change > slot) {
        overflow = true;
      } else {
        requests[key] =
            create_new_replication_vector(0, MINIMUM_REPLICA_NUMBER, 1, 1);
      }
    }
  }

  change_replication_factor(requests, global_hash_ring_map, local_hash_ring_map,
                            routing_address, placement, pushers, mt,
                            response_puller, logger, rid);
  logger->info("Demoting {} keys into {} EBS slots.", total_rep_to_change,
               slot);
  if (overflow && adding_ebs_node == 0 && time_elapsed > GRACE_PERIOD) {
    unsigned long long demote_data_size = total_rep_to_change * VALUE_SIZE;
    unsigned total_ebs_node_needed =
        ceil((ss.total_ebs_consumption + demote_data_size) /
             (EBS_CAPACITY_MAX * tier_data_map[2].node_capacity_));

    if (total_ebs_node_needed > ebs_node_number) {
      unsigned node_to_add = (total_ebs_node_needed - ebs_node_number);
      add_node(logger, "ebs", node_to_add, adding_ebs_node, management_address);
    }
  }

  requests.clear();
  total_rep_to_change = 0;

  // reduce the replication factor of some keys that are not so hot anymore
  for (auto it = key_access_summary.begin(); it != key_access_summary.end();
       it++) {
    std::string key = it->first;
    unsigned total_access = it->second;

    if (!is_metadata(key) && total_access <= ss.key_access_mean &&
        placement[key].global_replication_map_[1] > MINIMUM_REPLICA_NUMBER) {
      logger->info("Key {} accessed {} times (threshold is {}).", key,
                   total_access, ss.key_access_mean);
      requests[key] =
          create_new_replication_vector(1, MINIMUM_REPLICA_NUMBER - 1, 1, 1);
      logger->info("Dereplication for key {}. M: {}->{}. E: {}->{}", key,
                   placement[key].global_replication_map_[1],
                   requests[key].global_replication_map_[1],
                   placement[key].global_replication_map_[2],
                   requests[key].global_replication_map_[2]);
    }
  }

  change_replication_factor(requests, global_hash_ring_map, local_hash_ring_map,
                            routing_address, placement, pushers, mt,
                            response_puller, logger, rid);
  requests.clear();
}
