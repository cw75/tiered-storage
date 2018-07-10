#include "monitor/monitoring_utils.hpp"
#include "monitor/policies.hpp"

void storage_policy(
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::chrono::time_point<std::chrono::system_clock>& grace_start,
    SummaryStats& ss, unsigned& memory_node_number, unsigned& ebs_node_number,
    unsigned& adding_memory_node, unsigned& adding_ebs_node,
    bool& removing_ebs_node, Address management_address, MonitoringThread& mt,
    std::unordered_map<unsigned, TierData>& tier_data_map,
    std::unordered_map<Address, unsigned>& departing_node_map,
    SocketCache& pushers) {
  // check storage consumption and trigger elasticity if necessary
  if (adding_memory_node == 0 && ss.required_memory_node > memory_node_number) {
    auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now() - grace_start)
                            .count();
    if (time_elapsed > kGracePeriod) {
      add_node(logger, "memory", kNodeAdditionBatchSize, adding_memory_node,
               management_address);
    }
  }

  if (adding_ebs_node == 0 && ss.required_ebs_node > ebs_node_number) {
    auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now() - grace_start)
                            .count();
    if (time_elapsed > kGracePeriod) {
      add_node(logger, "ebs", kNodeAdditionBatchSize, adding_ebs_node,
               management_address);
    }
  }

  if (ss.avg_ebs_consumption_percentage < kMinEbsNodeConsumption &&
      !removing_ebs_node &&
      ebs_node_number >
          std::max(ss.required_ebs_node, (unsigned)kMinEbsTierSize)) {
    auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now() - grace_start)
                            .count();

    if (time_elapsed > kGracePeriod) {
      // pick a random ebs node and send remove node command
      auto node = next(begin(global_hash_ring_map[2]),
                       rand() % global_hash_ring_map[2].size())
                      ->second;
      remove_node(logger, node, "ebs", removing_ebs_node, pushers,
                  departing_node_map, mt, tier_data_map);
    }
  }
}
