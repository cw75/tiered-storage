//  Copyright 2018 U.C. Berkeley RISE Lab
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#include "route/routing_handlers.hpp"

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
        for (const auto& global_pair : global_hash_ring_map) {
          unsigned tier_id = global_pair.first;
          auto hash_ring = global_pair.second;
          std::unordered_set<Address> observed_ip;

          for (const auto& hash_pair : hash_ring) {
            std::string server_ip = hash_pair.second.get_ip();

            // if the node is not the newly joined node, send the ip of the
            // newly joined node
            if (server_ip.compare(new_server_ip) != 0 &&
                observed_ip.find(server_ip) == observed_ip.end()) {
              zmq_util::send_string(
                  std::to_string(tier) + ":" + new_server_ip,
                  &pushers[hash_pair.second.get_node_join_connect_addr()]);
              observed_ip.insert(server_ip);
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

    for (const auto& global_pair : global_hash_ring_map) {
      logger->info("Hash ring for tier {} size is {}.",
                   std::to_string(global_pair.first),
                   std::to_string(global_pair.second.size()));
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

    for (const auto& global_pair : global_hash_ring_map) {
      logger->info("Hash ring for tier {} size is {}.",
                   std::to_string(global_pair.first),
                   std::to_string(global_pair.second.size()));
    }
  }
}
