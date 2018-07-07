#include "spdlog/spdlog.h"
#include "hash_ring.hpp"
#include "monitor/monitoring_utils.hpp"

using namespace std;

void
policy_algorithm(shared_ptr<spdlog::logger> logger,
                 unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
                 unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
                 chrono::time_point<chrono::system_clock>& grace_start,
                 SummaryStats& ss,
                 unsigned& adding_memory_node,
                 unsigned& adding_ebs_node,
                 bool& removing_memory_node,
                 bool& removing_ebs_node,
                 string management_address,
                 unordered_map<string, KeyInfo>& placement,
                 unordered_map<string, unsigned>& key_access_summary,
                 MonitoringThread& mt,
                 unordered_map<unsigned, TierData>& tier_data_map,
                 unordered_map<string, unsigned>& departing_node_map,
                 SocketCache& pushers,
                 zmq::socket_t& response_puller,
                 vector<string>& routing_address,
                 unsigned& rid,
                 unordered_map<string, pair<double, unsigned>>& rep_factor_map,
                 const unsigned MINIMUM_MEMORY_NODE,
                 const unsigned MINIMUM_EBS_NODE,
                 const unsigned PROMOTE_THRESHOLD,
                 const unsigned DEMOTE_THRESHOLD,
                 const double MEM_CAPACITY_MAX,
                 const double EBS_CAPACITY_MAX,
                 const double EBS_CAPACITY_MIN,
                 const unsigned VALUE_SIZE,
                 const unsigned VIRTUAL_THREAD_NUM,
                 const unsigned GRACE_PERIOD,
                 const unsigned NODE_ADD
                 ) {
  unsigned memory_node_number = global_hash_ring_map[1].size() / VIRTUAL_THREAD_NUM;
  unsigned ebs_node_number = global_hash_ring_map[2].size() / VIRTUAL_THREAD_NUM;

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
}