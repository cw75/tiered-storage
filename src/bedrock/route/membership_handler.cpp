#include "hash_ring.hpp"
#include "spdlog/spdlog.h"

void membership_handler(
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* notify_puller,
    SocketCache& pushers,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unsigned thread_id, Address ip) {
  std::string message = zmq_util::recv_string(notify_puller);
  std::vector<std::string> v;

  split(message, ':', v);
  std::string type = v[0];
  unsigned tier = stoi(v[1]);
  Address new_server_ip = v[2];

  if (type == "join") {
    logger->info("Received join from server {} in tier {}.", new_server_ip,
                 std::to_string(tier));

    // update hash ring
    bool inserted = insert_to_hash_ring<GlobalHashRing>(
        global_hash_ring_map[tier], new_server_ip, 0);

    if (inserted) {
      if (thread_id == 0) {
        // gossip the new node address between server nodes to ensure
        // consistency
        for (auto it = global_hash_ring_map.begin();
             it != global_hash_ring_map.end(); it++) {
          unsigned tier_id = it->first;
          auto hash_ring = &(it->second);
          std::unordered_set<Address> observed_ip;

          for (auto iter = hash_ring->begin(); iter != hash_ring->end();
               iter++) {
            // if the node is not the newly joined node, send the ip of the
            // newly joined node
            if (iter->second.get_ip().compare(new_server_ip) != 0 &&
                observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
              zmq_util::send_string(
                  std::to_string(tier) + ":" + new_server_ip,
                  &pushers[(iter->second).get_node_join_connect_addr()]);
              observed_ip.insert(iter->second.get_ip());
            }
          }
        }

        // tell all worker threads about the message
        for (unsigned tid = 1; tid < kRoutingThreadCount; tid++) {
          zmq_util::send_string(
              message,
              &pushers[RoutingThread(ip, tid).get_notify_connect_addr()]);
        }
      }
    }

    for (auto it = global_hash_ring_map.begin();
         it != global_hash_ring_map.end(); it++) {
      logger->info("Hash ring for tier {} size is {}.",
                   std::to_string(it->first),
                   std::to_string(it->second.size()));
    }
  } else if (type == "depart") {
    logger->info("Received depart from server {}.", new_server_ip);
    remove_from_hash_ring<GlobalHashRing>(global_hash_ring_map[tier],
                                          new_server_ip, 0);

    if (thread_id == 0) {
      // tell all worker threads about the message
      for (unsigned tid = 1; tid < kRoutingThreadCount; tid++) {
        zmq_util::send_string(
            message,
            &pushers[RoutingThread(ip, tid).get_notify_connect_addr()]);
      }
    }

    for (auto it = global_hash_ring_map.begin();
         it != global_hash_ring_map.end(); it++) {
      logger->info("Hash ring for tier {} size is {}.",
                   std::to_string(it->first),
                   std::to_string(it->second.size()));
    }
  }
}
