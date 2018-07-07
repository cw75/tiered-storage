#include <fstream>
#include <chrono>
#include "zmq/socket_cache.hpp"
#include "common.hpp"
#include "hash_ring.hpp"
#include "kvs/rc_pair_lattice.hpp"
#include "kvs/kvs_handlers.hpp"

void process_gossip(string gossip_string,
    ServerThread& wt,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    unordered_map<string, KeyInfo>& placement,
    SocketCache& pushers,
    Serializer* serializer,
    unordered_map<string, KeyStat>& key_stat_map,
    unordered_map<string, pair<chrono::system_clock::time_point, vector<PendingGossip>>>& pending_gossip_map,
    unsigned& seed) {

  communication::Request gossip;
  gossip.ParseFromString(gossip_string);

  vector<unsigned> tier_ids = { SELF_TIER_ID };
  bool succeed;
  unordered_map<string, communication::Request> gossip_map;

  for (int i = 0; i < gossip.tuple_size(); i++) {
    // first check if the thread is responsible for the key
    string key = gossip.tuple(i).key();
    auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

    if (succeed) {
      if (threads.find(wt) != threads.end()) { // this means this worker thread is one of the responsible threads
        process_put(gossip.tuple(i).key(), gossip.tuple(i).timestamp(), gossip.tuple(i).value(), serializer, key_stat_map);
      } else {
        if (is_metadata(key)) { // forward the gossip
          for (auto it = threads.begin(); it != threads.end(); it++) {
            if (gossip_map.find(it->get_gossip_connect_addr()) == gossip_map.end()) {
              gossip_map[it->get_gossip_connect_addr()].set_type("PUT");
            }

            prepare_put_tuple(gossip_map[it->get_gossip_connect_addr()], key, gossip.tuple(i).value(), gossip.tuple(i).timestamp());
          }
        } else {
          issue_replication_factor_request(wt.get_replication_factor_connect_addr(), key, global_hash_ring_map[1], local_hash_ring_map[1], pushers, seed);

          if (pending_gossip_map.find(key) == pending_gossip_map.end()) {
            pending_gossip_map[key].first = chrono::system_clock::now();
          }

          pending_gossip_map[key].second.push_back(PendingGossip(gossip.tuple(i).value(), gossip.tuple(i).timestamp()));
        }
      }
    } else {
      if (pending_gossip_map.find(key) == pending_gossip_map.end()) {
        pending_gossip_map[key].first = chrono::system_clock::now();
      }

      pending_gossip_map[key].second.push_back(PendingGossip(gossip.tuple(i).value(), gossip.tuple(i).timestamp()));
    }
  }

  // redirect gossip
  for (auto it = gossip_map.begin(); it != gossip_map.end(); it++) {
    push_request(it->second, pushers[it->first]);
  }
}
