#include <chrono>

#include "kvs/kvs_handlers.hpp"

void gossip_handler(
    unsigned& seed, zmq::socket_t* gossip_puller,
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, unsigned>& key_size_map,
    PendingMap<PendingGossip>& pending_gossip_map,
    std::unordered_map<Key, KeyInfo>& placement, ServerThread& wt,
    Serializer* serializer, SocketCache& pushers) {

  std::string gossip_string = zmq_util::recv_string(gossip_puller);
  KeyRequest gossip;
  gossip.ParseFromString(gossip_string);
  logger->info("Recevied gossip.");

  bool succeed;
  std::unordered_map<Address, KeyRequest> gossip_map;

  for (const KeyTuple& tuple : gossip.tuples()) {
    // first check if the thread is responsible for the key
    Key key = tuple.key();
    ServerThreadSet threads = get_responsible_threads(
        wt.get_replication_factor_connect_addr(), key, is_metadata(key),
        global_hash_ring_map, local_hash_ring_map, placement, pushers,
        kSelfTierIdVector, succeed, seed);

    if (succeed) {
      if (threads.find(wt) !=
          threads.end()) {  // this means this worker thread is one of the
                            // responsible threads
        process_put(tuple.key(), tuple.timestamp(), tuple.value(), serializer,
                    key_size_map);
      } else {
        if (is_metadata(key)) {  // forward the gossip
          for (const ServerThread& thread : threads) {
            if (gossip_map.find(thread.get_gossip_connect_addr()) ==
                gossip_map.end()) {
              gossip_map[thread.get_gossip_connect_addr()].set_type(get_request_type("PUT"));
            }

            prepare_put_tuple(gossip_map[thread.get_gossip_connect_addr()], key,
                              tuple.value(), tuple.timestamp());
          }
          logger->info("Forwarded metadata gossip.");
        } else {
          issue_replication_factor_request(
              wt.get_replication_factor_connect_addr(), key,
              global_hash_ring_map[1], local_hash_ring_map[1], pushers, seed);

          pending_gossip_map[key].push_back(
              PendingGossip(tuple.value(), tuple.timestamp()));

          logger->info("Issued replication factor request.");
        }
      }
    } else {
      pending_gossip_map[key].push_back(
          PendingGossip(tuple.value(), tuple.timestamp()));
    }
  }

  // redirect gossip
  for (const auto& gossip_pair : gossip_map) {
    push_request(gossip_pair.second, pushers[gossip_pair.first]);
  }
}
