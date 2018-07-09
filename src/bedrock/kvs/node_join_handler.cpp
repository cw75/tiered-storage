#include <fstream>
#include <string>

#include "common.hpp"
#include "hash_ring.hpp"
#include "kvs/kvs_handlers.hpp"
#include "kvs/rc_pair_lattice.hpp"
#include "spdlog/spdlog.h"
#include "zmq/socket_cache.hpp"

void node_join_handler(
    unsigned int thread_num, unsigned thread_id, unsigned& seed, std::string ip,
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* join_puller,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<std::string, unsigned>& key_size_map,
    std::unordered_map<std::string, KeyInfo>& placement,
    std::unordered_set<std::string>& join_remove_set, SocketCache& pushers,
    ServerThread& wt, AddressKeysetMap& join_addr_keyset_map) {
  std::string message = zmq_util::recv_string(join_puller);
  std::vector<std::string> v;
  split(message, ':', v);
  unsigned tier = stoi(v[0]);
  std::string new_server_ip = v[1];

  // update global hash ring
  bool inserted = insert_to_hash_ring<GlobalHashRing>(
      global_hash_ring_map[tier], new_server_ip, 0);

  if (inserted) {
    logger->info("Received a node join for tier {}. New node is {}", tier,
                 new_server_ip);

    // only thread 0 communicates with other nodes and receives distributed
    // join messages; it communicates that information to non-0 threads on its
    // own machine
    if (thread_id == 0) {
      // send my IP to the new server node
      zmq_util::send_string(std::to_string(kSelfTierId) + ":" + ip,
                            &pushers[ServerThread(new_server_ip, 0)
                                         .get_node_join_connect_addr()]);

      // gossip the new node address between server nodes to ensure consistency
      for (auto it = global_hash_ring_map.begin();
           it != global_hash_ring_map.end(); ++it) {
        auto hash_ring = &(it->second);
        std::unordered_set<std::string> observed_ip;

        for (auto iter = hash_ring->begin(); iter != hash_ring->end(); iter++) {
          // if the node is not myself and not the newly joined node, send the
          // ip of the newly joined node in case of a race condition
          if (iter->second.get_ip().compare(ip) != 0 &&
              iter->second.get_ip().compare(new_server_ip) != 0 &&
              observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
            zmq_util::send_string(
                message, &pushers[(iter->second).get_node_join_connect_addr()]);
            observed_ip.insert(iter->second.get_ip());
          }
        }

        logger->info("Hash ring for tier {} is size {}.",
                     std::to_string(it->first),
                     std::to_string(it->second.size()));
      }

      // tell all worker threads about the new node join
      for (unsigned tid = 1; tid < thread_num; tid++) {
        zmq_util::send_string(
            message,
            &pushers[ServerThread(ip, tid).get_node_join_connect_addr()]);
      }
    }

    if (tier == kSelfTierId) {
      bool succeed;

      for (auto it = key_size_map.begin(); it != key_size_map.end(); it++) {
        std::string key = it->first;
        auto threads = get_responsible_threads(
            wt.get_replication_factor_connect_addr(), key, is_metadata(key),
            global_hash_ring_map, local_hash_ring_map, placement, pushers,
            kSelfTierIdVector, succeed, seed);

        if (succeed) {
          if (threads.find(wt) == threads.end()) {
            join_remove_set.insert(key);

            for (auto iter = threads.begin(); iter != threads.end(); iter++) {
              join_addr_keyset_map[iter->get_gossip_connect_addr()].insert(key);
            }
          }
        } else {
          logger->info(
              "Error: Missing key replication factor in node join "
              "routine. This should never happen.");
        }
      }
    }
  }
}
