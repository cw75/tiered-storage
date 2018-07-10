#include "monitor/monitoring_utils.hpp"
#include "monitor/policies.hpp"

void movement_policy(
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::chrono::time_point<std::chrono::system_clock>& grace_start,
    SummaryStats& ss, unsigned& memory_node_number, unsigned& ebs_node_number,
    unsigned& adding_memory_node, unsigned& adding_ebs_node,
    Address management_address, std::unordered_map<Key, KeyInfo>& placement,
    std::unordered_map<Key, unsigned>& key_access_summary, MonitoringThread& mt,
    std::unordered_map<unsigned, TierData>& tier_data_map, SocketCache& pushers,
    zmq::socket_t& response_puller, std::vector<Address>& routing_address,
    unsigned& rid) {
  // promote hot keys to memory tier
  std::unordered_map<Key, KeyInfo> requests;
  unsigned total_rep_to_change = 0;
  unsigned slot = (kMaxMemoryNodeConsumption * tier_data_map[1].node_capacity_ *
                       memory_node_number -
                   ss.total_memory_consumption) /
                  kValueSize;
  bool overflow = false;

  for (const auto& key_access_pair : key_access_summary) {
    Key key = key_access_pair.first;
    unsigned total_access = key_access_pair.second;

    if (!is_metadata(key) && total_access > kKeyPromotionThreshold &&
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

  if (overflow && adding_memory_node == 0 && time_elapsed > kGracePeriod) {
    unsigned long long promote_data_size = total_rep_to_change * kValueSize;
    unsigned total_memory_node_needed =
        ceil((ss.total_memory_consumption + promote_data_size) /
             (kMaxMemoryNodeConsumption * tier_data_map[1].node_capacity_));

    if (total_memory_node_needed > memory_node_number) {
      unsigned node_to_add = (total_memory_node_needed - memory_node_number);
      add_node(logger, "memory", node_to_add, adding_memory_node,
               management_address);
    }
  }

  requests.clear();
  total_rep_to_change = 0;

  // demote cold keys to ebs tier
  slot = (kMaxEbsNodeConsumption * tier_data_map[2].node_capacity_ *
              ebs_node_number -
          ss.total_ebs_consumption) /
         kValueSize;
  overflow = false;

  for (const auto& key_access_pair : key_access_summary) {
    Key key = key_access_pair.first;
    unsigned total_access = key_access_pair.second;

    if (!is_metadata(key) && total_access < kKeyDemotionThreshold &&
        placement[key].global_replication_map_[1] > 0) {
      total_rep_to_change += 1;

      if (total_rep_to_change > slot) {
        overflow = true;
      } else {
        requests[key] =
            create_new_replication_vector(0, kMinimumReplicaNumber, 1, 1);
      }
    }
  }

  change_replication_factor(requests, global_hash_ring_map, local_hash_ring_map,
                            routing_address, placement, pushers, mt,
                            response_puller, logger, rid);
  logger->info("Demoting {} keys into {} EBS slots.", total_rep_to_change,
               slot);
  if (overflow && adding_ebs_node == 0 && time_elapsed > kGracePeriod) {
    unsigned long long demote_data_size = total_rep_to_change * kValueSize;
    unsigned total_ebs_node_needed =
        ceil((ss.total_ebs_consumption + demote_data_size) /
             (kMaxEbsNodeConsumption * tier_data_map[2].node_capacity_));

    if (total_ebs_node_needed > ebs_node_number) {
      unsigned node_to_add = (total_ebs_node_needed - ebs_node_number);
      add_node(logger, "ebs", node_to_add, adding_ebs_node, management_address);
    }
  }

  requests.clear();
  total_rep_to_change = 0;

  // reduce the replication factor of some keys that are not so hot anymore
  for (const auto& key_access_pair : key_access_summary) {
    Key key = key_access_pair.first;
    unsigned total_access = key_access_pair.second;

    if (!is_metadata(key) && total_access <= ss.key_access_mean &&
        placement[key].global_replication_map_[1] > kMinimumReplicaNumber) {
      logger->info("Key {} accessed {} times (threshold is {}).", key,
                   total_access, ss.key_access_mean);
      requests[key] =
          create_new_replication_vector(1, kMinimumReplicaNumber - 1, 1, 1);
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
