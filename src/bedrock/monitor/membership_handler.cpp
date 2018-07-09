#include "hash_ring.hpp"
#include "spdlog/spdlog.h"

void membership_handler(
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* notify_puller,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unsigned& adding_memory_node, unsigned& adding_ebs_node,
    std::chrono::time_point<std::chrono::system_clock>& grace_start,
    std::vector<std::string>& routing_address,
    std::unordered_map<std::string,
                       std::unordered_map<unsigned, unsigned long long>>&
        memory_tier_storage,
    std::unordered_map<std::string,
                       std::unordered_map<unsigned, unsigned long long>>&
        ebs_tier_storage,
    std::unordered_map<
        std::string, std::unordered_map<unsigned, std::pair<double, unsigned>>>&
        memory_tier_occupancy,
    std::unordered_map<
        std::string, std::unordered_map<unsigned, std::pair<double, unsigned>>>&
        ebs_tier_occupancy,
    std::unordered_map<std::string, std::unordered_map<std::string, unsigned>>&
        key_access_frequency) {
  std::string message = zmq_util::recv_string(notify_puller);
  std::vector<std::string> v;

  split(message, ':', v);
  std::string type = v[0];
  unsigned tier = stoi(v[1]);
  std::string new_server_ip = v[2];

  if (type == "join") {
    logger->info("Received join from server {} in tier {}.", new_server_ip,
                 std::to_string(tier));
    if (tier == 1) {
      insert_to_hash_ring<GlobalHashRing>(global_hash_ring_map[tier],
                                          new_server_ip, 0);

      if (adding_memory_node > 0) {
        adding_memory_node -= 1;
      }

      // reset grace period timer
      grace_start = std::chrono::system_clock::now();
    } else if (tier == 2) {
      insert_to_hash_ring<GlobalHashRing>(global_hash_ring_map[tier],
                                          new_server_ip, 0);

      if (adding_ebs_node > 0) {
        adding_ebs_node -= 1;
      }

      // reset grace period timer
      grace_start = std::chrono::system_clock::now();
    } else if (tier == 0) {
      routing_address.push_back(new_server_ip);
    } else {
      logger->error("Invalid tier: {}.", std::to_string(tier));
    }

    for (auto it = global_hash_ring_map.begin();
         it != global_hash_ring_map.end(); it++) {
      logger->info("Hash ring for tier {} is size {}.",
                   std::to_string(it->first),
                   std::to_string(it->second.size()));
    }
  } else if (type == "depart") {
    logger->info("Received depart from server {}.", new_server_ip);

    // update hash ring
    if (tier == 1) {
      remove_from_hash_ring<GlobalHashRing>(global_hash_ring_map[tier],
                                            new_server_ip, 0);
      memory_tier_storage.erase(new_server_ip);
      memory_tier_occupancy.erase(new_server_ip);

      for (auto it = key_access_frequency.begin();
           it != key_access_frequency.end(); it++) {
        for (unsigned i = 0; i < kMemoryThreadCount; i++) {
          it->second.erase(new_server_ip + ":" + std::to_string(i));
        }
      }
    } else if (tier == 2) {
      remove_from_hash_ring<GlobalHashRing>(global_hash_ring_map[tier],
                                            new_server_ip, 0);
      ebs_tier_storage.erase(new_server_ip);
      ebs_tier_occupancy.erase(new_server_ip);

      for (auto it = key_access_frequency.begin();
           it != key_access_frequency.end(); it++) {
        for (unsigned i = 0; i < kEbsThreadCount; i++) {
          it->second.erase(new_server_ip + ":" + std::to_string(i));
        }
      }
    } else {
      logger->error("Invalid tier: {}.", std::to_string(tier));
    }

    for (auto it = global_hash_ring_map.begin();
         it != global_hash_ring_map.end(); it++) {
      logger->info("Hash ring for tier {} is size {}.",
                   std::to_string(it->first),
                   std::to_string(it->second.size()));
    }
  }
}
