#include <chrono>
#include <fstream>

#include "common.hpp"
#include "hash_ring.hpp"
#include "kvs/kvs_handlers.hpp"
#include "kvs/rc_pair_lattice.hpp"
#include "zmq/socket_cache.hpp"

void rep_factor_change_handler(
    std::string ip, unsigned thread_id, unsigned thread_num, unsigned& seed,
    std::shared_ptr<spdlog::logger> logger,
    zmq::socket_t* rep_factor_change_puller,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<std::string, KeyInfo> placement,
    std::unordered_map<std::string, KeyStat>& key_stat_map,
    std::unordered_set<std::string>& local_changeset, ServerThread& wt,
    Serializer* serializer, SocketCache& pushers) {
  std::string change_string = zmq_util::recv_string(rep_factor_change_puller);

  // TODO(vikram): make logging in all handlers consistent
  logger->info("Received replication factor change.");
  if (thread_id == 0) {
    // tell all worker threads about the replication factor change
    for (unsigned tid = 1; tid < thread_num; tid++) {
      zmq_util::send_string(
          change_string,
          &pushers[ServerThread(ip, tid)
                       .get_replication_factor_change_connect_addr()]);
    }
  }

  communication::Replication_Factor_Request req;
  req.ParseFromString(change_string);

  AddressKeysetMap addr_keyset_map;
  std::unordered_set<std::string> remove_set;

  // for every key, update the replication factor and check if the node is still
  // responsible for the key
  // TODO: make a global vec with all tier ids once instead of
  // redefining it everywhere, as well as a global vec with just my tier's id
  std::vector<unsigned> tier_ids;
  for (unsigned i = kMinTier; i <= kMaxTier; i++) {
    tier_ids.push_back(i);
  }

  bool succeed;

  for (int i = 0; i < req.tuple_size(); i++) {
    auto curr_tuple = req.tuple(i);
    std::string key = curr_tuple.key();

    // if this thread was responsible for the key before the change
    if (key_stat_map.find(key) != key_stat_map.end()) {
      auto orig_threads = get_responsible_threads(
          wt.get_replication_factor_connect_addr(), key, is_metadata(key),
          global_hash_ring_map, local_hash_ring_map, placement, pushers,
          tier_ids, succeed, seed);

      if (succeed) {
        // update the replication factor
        bool decrement = false;

        for (int j = 0; j < req.tuple(i).global_size(); j++) {
          auto curr_global = curr_tuple.global(j);
          if (curr_global.global_replication() <
              placement[key].global_replication_map_[curr_global.tier_id()]) {
            decrement = true;
          }

          placement[key].global_replication_map_[curr_global.tier_id()] =
              curr_global.global_replication();
        }

        for (int j = 0; j < curr_tuple.local_size(); j++) {
          auto curr_local = curr_tuple.local(j);
          if (curr_local.local_replication() <
              placement[key].local_replication_map_[curr_local.tier_id()]) {
            decrement = true;
          }

          placement[key].local_replication_map_[curr_local.tier_id()] =
              curr_local.local_replication();
        }

        auto threads = get_responsible_threads(
            wt.get_replication_factor_connect_addr(), key, is_metadata(key),
            global_hash_ring_map, local_hash_ring_map, placement, pushers,
            tier_ids, succeed, seed);

        if (succeed) {
          if (threads.find(wt) == threads.end()) {  // this thread is no longer
                                                    // responsible for this key
            remove_set.insert(key);

            // add all the new threads that this key should be sent to
            for (auto it = threads.begin(); it != threads.end(); it++) {
              addr_keyset_map[it->get_gossip_connect_addr()].insert(key);
            }
          }

          // decrement represents whether the total global or local rep factor
          // has been reduced; if that's not the case, and I am the "first"
          // thread responsible for this key, then I gossip it to the new
          // threads that are responsible for it
          if (!decrement && orig_threads.begin()->get_id() == wt.get_id()) {
            std::unordered_set<ServerThread, ThreadHash> new_threads;

            for (auto it = threads.begin(); it != threads.end(); it++) {
              if (orig_threads.find(*it) == orig_threads.end()) {
                new_threads.insert(*it);
              }
            }

            for (auto it = new_threads.begin(); it != new_threads.end(); it++) {
              addr_keyset_map[it->get_gossip_connect_addr()].insert(key);
            }
          }
        } else {
          logger->error(
              "Missing key replication factor in rep factor change routine.");
        }
      } else {
        logger->error(
            "Missing key replication factor in rep factor change routine.");

        // just update the replication factor
        for (int j = 0; j < curr_tuple.global_size(); j++) {
          placement[key]
              .global_replication_map_[curr_tuple.global(j).tier_id()] =
              curr_tuple.global(j).global_replication();
        }

        for (int j = 0; j < curr_tuple.local_size(); j++) {
          placement[key].local_replication_map_[curr_tuple.local(j).tier_id()] =
              curr_tuple.local(j).local_replication();
        }
      }
    } else {
      // just update the replication factor
      for (int j = 0; j < curr_tuple.global_size(); j++) {
        placement[key].global_replication_map_[curr_tuple.global(j).tier_id()] =
            curr_tuple.global(j).global_replication();
      }

      for (int j = 0; j < curr_tuple.local_size(); j++) {
        placement[key].local_replication_map_[curr_tuple.local(j).tier_id()] =
            curr_tuple.local(j).local_replication();
      }
    }
  }

  send_gossip(addr_keyset_map, pushers, serializer);

  // remove keys
  for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
    key_stat_map.erase(*it);
    serializer->remove(*it);
    local_changeset.erase(*it);
  }
}
