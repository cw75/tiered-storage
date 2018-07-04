#include <zmq.hpp>
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <cstdio>
#include <chrono>
#include "zmq/socket_cache.h"
#include "kvs/rc_kv_store.h"
#include "common.h"
#include "server.h"

using namespace std;

void
join_handler(string ip,
             unsigned int self_tier_id,
             unsigned int thread_num,
             zmq::socket_t *join_puller_ref,
             SocketCache& pushers,
             unordered_map<unsigned, global_hash_t>& global_hash_ring_map,
             unordered_map<unsigned, local_hash_t>& local_hash_ring_map,
             unordered_map<string, key_info> placement,
             std::shared_ptr<spdlog::logger> logger,
             unsigned thread_id,
             unsigned &seed,
             unordered_map<string, key_stat>& key_stat_map,
             server_thread_t& wt,
             address_keyset_map join_addr_keyset_map,
             unordered_set<string> join_remove_set
             ) {
  string message = zmq_util::recv_string(join_puller_ref);

  vector<string> v;
  split(message, ':', v);
  unsigned tier = stoi(v[0]);
  string new_server_ip = v[1];

  // update global hash ring
  bool inserted = insert_to_hash_ring<global_hash_t>(global_hash_ring_map[tier], new_server_ip, 0);

  if (inserted) {
    logger->info("Received a node join for tier {}. New node is {}", tier, new_server_ip);

    // only relevant to thread 0
    if (thread_id == 0) {
      // gossip the new node address between server nodes to ensure consistency
      for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
        unsigned tier_id = it->first;
        auto hash_ring = &(it->second);
        unordered_set<string> observed_ip;
        for (auto iter = hash_ring->begin(); iter != hash_ring->end(); iter++) {
          if (iter->second.get_ip().compare(ip) != 0 && iter->second.get_ip().compare(new_server_ip) != 0 && observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
            // if the node is not myself and not the newly joined node, send the ip of the newly joined node
            zmq_util::send_string(message, &pushers[(iter->second).get_node_join_connect_addr()]);
            observed_ip.insert(iter->second.get_ip());
          } else if (iter->second.get_ip().compare(new_server_ip) == 0 && observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
            // if the node is the newly joined node, send my ip
            zmq_util::send_string(to_string(self_tier_id) + ":" + ip, &pushers[(iter->second).get_node_join_connect_addr()]);
            observed_ip.insert(iter->second.get_ip());
          }
        }
      }

      // tell all worker threads about the new node join
      for (unsigned tid = 1; tid < thread_num; tid++) {
        zmq_util::send_string(message, &pushers[server_thread_t(ip, tid).get_node_join_connect_addr()]);
      }

      for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
        logger->info("Hash ring for tier {} is size {}.", to_string(it->first), to_string(it->second.size()));
      }
    }

    if (tier == self_tier_id) {
      vector<unsigned> tier_ids;
      tier_ids.push_back(self_tier_id);
      bool succeed;

      for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
        string key = it->first;
        auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

        if (succeed) {
          if (threads.find(wt) == threads.end()) {
            join_remove_set.insert(key);

            for (auto iter = threads.begin(); iter != threads.end(); iter++) {
              join_addr_keyset_map[iter->get_gossip_connect_addr()].insert(key);
            }
          }
        } else {
          logger->info("Error: missing key replication factor in node join routine.");
        }
      }
    }
  }
}