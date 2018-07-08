#include "spdlog/spdlog.h"
#include "hash_ring.hpp"
#include "monitor/monitoring_utils.hpp"

using namespace std;

void
storage_policy(shared_ptr<spdlog::logger> logger,
               unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
               chrono::time_point<chrono::system_clock>& grace_start,
               SummaryStats& ss,
               unsigned& memory_node_number,
               unsigned& ebs_node_number,
               unsigned& adding_memory_node,
               unsigned& adding_ebs_node,
               bool& removing_ebs_node,
               string management_address,
               MonitoringThread& mt,
               unordered_map<unsigned, TierData>& tier_data_map,
               unordered_map<string, unsigned>& departing_node_map,
               SocketCache& pushers
               ) {
  // check storage consumption and trigger elasticity if necessary
  if (adding_memory_node == 0 && ss.required_memory_node > memory_node_number) {
    auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-grace_start).count();
    if (time_elapsed > GRACE_PERIOD) {
      add_node(logger, "memory", NODE_ADD, adding_memory_node, management_address);
    }
  }

  if (adding_ebs_node == 0 && ss.required_ebs_node > ebs_node_number) {
    auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-grace_start).count();
    if (time_elapsed > GRACE_PERIOD) {
      add_node(logger, "ebs", NODE_ADD, adding_ebs_node, management_address);
    }
  }

  if (ss.avg_ebs_consumption_percentage < EBS_CAPACITY_MIN && !removing_ebs_node && ebs_node_number > max(ss.required_ebs_node, (unsigned)MINIMUM_EBS_NODE)) {
    auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-grace_start).count();

    if (time_elapsed > GRACE_PERIOD) {
      // pick a random ebs node and send remove node command
      auto node = next(begin(global_hash_ring_map[2]), rand() % global_hash_ring_map[2].size())->second;
      remove_node(logger,
                  node,
                  "ebs",
                  removing_ebs_node,
                  pushers,
                  departing_node_map,
                  mt,
                  tier_data_map);
    }
  }
}