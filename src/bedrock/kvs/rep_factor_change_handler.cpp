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
    std::unordered_map<std::string, unsigned>& key_size_map,
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
  bool succeed;

  for (const auto& curr_tuple : req.tuple()) {
    std::string key = curr_tuple.key();

    // if this thread was responsible for the key before the change
    if (key_size_map.find(key) != key_size_map.end()) {
      ServerThreadSet orig_threads = get_responsible_threads(
          wt.get_replication_factor_connect_addr(), key, is_metadata(key),
          global_hash_ring_map, local_hash_ring_map, placement, pushers,
          kAllTierIds, succeed, seed);

      if (succeed) {
        // update the replication factor
        bool decrement = false;

        for (const auto& global : curr_tuple.global()) {
          if (global.global_replication() <
              placement[key].global_replication_map_[global.tier_id()]) {
            decrement = true;
          }

          placement[key].global_replication_map_[global.tier_id()] =
              global.global_replication();
        }

        for (const auto& local : curr_tuple.local()) {
          if (local.local_replication() <
              placement[key].local_replication_map_[local.tier_id()]) {
            decrement = true;
          }

          placement[key].local_replication_map_[local.tier_id()] =
              local.local_replication();
        }

        ServerThreadSet threads = get_responsible_threads(
            wt.get_replication_factor_connect_addr(), key, is_metadata(key),
            global_hash_ring_map, local_hash_ring_map, placement, pushers,
            kAllTierIds, succeed, seed);

        if (succeed) {
          if (threads.find(wt) == threads.end()) {  // this thread is no longer
                                                    // responsible for this key
            remove_set.insert(key);

            // add all the new threads that this key should be sent to
            for (const ServerThread& thread : threads) {
              addr_keyset_map[thread.get_gossip_connect_addr()].insert(key);
            }
          }

          // decrement represents whether the total global or local rep factor
          // has been reduced; if that's not the case, and I am the "first"
          // thread responsible for this key, then I gossip it to the new
          // threads that are responsible for it
          if (!decrement && orig_threads.begin()->get_id() == wt.get_id()) {
            std::unordered_set<ServerThread, ThreadHash> new_threads;

            for (const ServerThread& thread : threads) {
              if (orig_threads.find(thread) == orig_threads.end()) {
                new_threads.insert(thread);
              }
            }


            for (const ServerThread& thread : new_threads) {
              addr_keyset_map[thread.get_gossip_connect_addr()].insert(key);
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
        for (const auto& global : curr_tuple.global()) {
          placement[key]
              .global_replication_map_[global.tier_id()] =
              global.global_replication();
        }

        for (const auto& local : curr_tuple.local()) {
          placement[key].local_replication_map_[local.tier_id()] =
              local.local_replication();
        }
      }
    } else {
      // just update the replication factor
      for (const auto& global : curr_tuple.global()) {
        placement[key].global_replication_map_[global.tier_id()] =
            global.global_replication();
      }

      for (const auto& local : curr_tuple.local()) {
        placement[key].local_replication_map_[local.tier_id()] =
            local.local_replication();
      }
    }
  }

  send_gossip(addr_keyset_map, pushers, serializer);

  // remove keys
  for (const std::string& key : remove_set) {
    key_size_map.erase(key);
    serializer->remove(key);
    local_changeset.erase(key);
  }
}
