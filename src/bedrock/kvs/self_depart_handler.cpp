#include <fstream>
#include <string>

#include "common.hpp"
#include "hash_ring.hpp"
#include "kvs/kvs_handlers.hpp"
#include "kvs/rc_pair_lattice.hpp"
#include "spdlog/spdlog.h"
#include "zmq/socket_cache.hpp"

void self_depart_handler(
    unsigned thread_id, unsigned& seed, std::string ip,
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* self_depart_puller,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<std::string, unsigned>& key_size_map,
    std::unordered_map<std::string, KeyInfo>& placement,
    std::vector<std::string> routing_address,
    std::vector<std::string> monitoring_address, ServerThread wt,
    SocketCache& pushers, Serializer* serializer) {
  std::string ack_addr = zmq_util::recv_string(self_depart_puller);
  logger->info("Node is departing.");
  remove_from_hash_ring<GlobalHashRing>(global_hash_ring_map[kSelfTierId], ip,
                                        0);

  // thread 0 notifies other nodes in the cluster (of all types) that it is
  // leaving the cluster
  if (thread_id == 0) {
    std::string msg = std::to_string(kSelfTierId) + ":" + ip;

    for (const auto& global_pair : global_hash_ring_map) {
      GlobalHashRing hash_ring = global_pair.second;
      std::unordered_set<std::string> observed_ip;

      for (const auto& hash_pair : hash_ring) {
        std::string this_ip = hash_pair.second.get_ip();

        if (observed_ip.find(this_ip) == observed_ip.end()) {
          zmq_util::send_string(
              msg, &pushers[hash_pair.second.get_node_depart_connect_addr()]);
          observed_ip.insert(this_ip);
        }
      }
    }

    msg = "depart:" + std::to_string(kSelfTierId) + ":" + ip;

    // notify all routing nodes
    for (const std::string& address : routing_address) {
      zmq_util::send_string(
          msg, &pushers[RoutingThread(address, 0).get_notify_connect_addr()]);
    }

    // notify monitoring nodes
    for (const std::string& address : monitoring_address) {
      zmq_util::send_string(
          msg, &pushers[MonitoringThread(address).get_notify_connect_addr()]);
    }

    // tell all worker threads about the self departure
    for (unsigned tid = 1; tid < kThreadNum; tid++) {
      zmq_util::send_string(
          ack_addr,
          &pushers[ServerThread(ip, tid).get_self_depart_connect_addr()]);
    }
  }

  AddressKeysetMap addr_keyset_map;
  bool succeed;

  for (const auto& key_pair : key_size_map) {
    std::string key = key_pair.first;
    ServerThreadSet threads = get_responsible_threads(
        wt.get_replication_factor_connect_addr(), key, is_metadata(key),
        global_hash_ring_map, local_hash_ring_map, placement, pushers, kAllTierIds,
        succeed, seed);

    if (succeed) {
      // since we already removed this node from the hash ring, no need to exclude
      // it explicitly
      for (const ServerThread& thread : threads) {
        addr_keyset_map[thread.get_gossip_connect_addr()].insert(key);
      }
    } else {
      logger->error("Missing key replication factor in node depart routine");
    }
  }

  send_gossip(addr_keyset_map, pushers, serializer);
  zmq_util::send_string(ip + "_" + std::to_string(kSelfTierId),
                        &pushers[ack_addr]);
}
