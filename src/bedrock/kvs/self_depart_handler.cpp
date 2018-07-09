#include <fstream>
#include <string>
#include "spdlog/spdlog.h"
#include "zmq/socket_cache.hpp"
#include "common.hpp"
#include "hash_ring.hpp"
#include "kvs/rc_pair_lattice.hpp"
#include "kvs/kvs_handlers.hpp"

void self_depart_handler(unsigned thread_num,
    unsigned thread_id,
    unsigned seed,
    string ip,
    std::shared_ptr<spdlog::logger> logger,
    zmq::socket_t* self_depart_puller,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    unordered_map<string, KeyStat>& key_stat_map,
    unordered_map<string, KeyInfo>& placement,
    vector<string> routing_address,
    vector<string> monitoring_address,
    ServerThread wt,
    SocketCache& pushers,
    Serializer* serializer) {

  string ack_addr = zmq_util::recv_string(self_depart_puller);
  logger->info("Node is departing.");
  remove_from_hash_ring<GlobalHashRing>(global_hash_ring_map[SELF_TIER_ID], ip, 0);

  // thread 0 notifies other nodes in the cluster (of all types) that it is
  // leaving the cluster
  if (thread_id == 0) {
    string msg = to_string(SELF_TIER_ID) + ":" + ip;

    for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
      auto hash_ring = &(it->second);
      unordered_set<string> observed_ip;

      for (auto iter = hash_ring->begin(); iter != hash_ring->end(); iter++) {
        if (observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
          zmq_util::send_string(msg, &pushers[(iter->second).get_node_depart_connect_addr()]);
          observed_ip.insert(iter->second.get_ip());
        }
      }
    }

    msg = "depart:" + to_string(SELF_TIER_ID) + ":" + ip;

    // notify all routing nodes
    for (auto it = routing_address.begin(); it != routing_address.end(); it++) {
      zmq_util::send_string(msg, &pushers[RoutingThread(*it, 0).get_notify_connect_addr()]);
    }

    // notify monitoring nodes
    for (auto it = monitoring_address.begin(); it != monitoring_address.end(); it++) {
      zmq_util::send_string(msg, &pushers[MonitoringThread(*it).get_notify_connect_addr()]);
    }

    // tell all worker threads about the self departure
    for (unsigned tid = 1; tid < thread_num; tid++) {
      zmq_util::send_string(ack_addr, &pushers[ServerThread(ip, tid).get_self_depart_connect_addr()]);
    }
  }

  AddressKeysetMap addr_keyset_map;
  vector<unsigned> tier_ids;

  for (unsigned i = MIN_TIER; i <= MAX_TIER; i++) {
    tier_ids.push_back(i);
  }

  bool succeed;

  for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
    string key = it->first;
    auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

    if (succeed) {
      // since we already removed itself from the hash ring, no need to exclude itself from threads
      for (auto iter = threads.begin(); iter != threads.end(); iter++) {
        addr_keyset_map[iter->get_gossip_connect_addr()].insert(key);
      }
    } else {
      logger->info("Error: key missing replication factor in node depart routine");
    }
  }

  send_gossip(addr_keyset_map, pushers, serializer);
  zmq_util::send_string(ip + "_" + to_string(SELF_TIER_ID), &pushers[ack_addr]);
}
