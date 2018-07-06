#ifndef __KVS_HANDLERS_H__
#define __KVS_HANDLERS_H__

#include "utils/server_utility.hpp"

void
join_handler(unsigned int self_tier_id,
             unsigned int thread_num,
             string ip,
             zmq::socket_t *join_puller_ref,
             SocketCache& pushers,
             unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
             unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
             unordered_map<string, KeyInfo>& placement,
             std::shared_ptr<spdlog::logger> logger,
             unsigned thread_id,
             unsigned &seed,
             unordered_map<string, KeyStat>& key_stat_map,
             ServerThread& wt,
             AddressKeysetMap& join_addr_keyset_map,
             unordered_set<string>& join_remove_set
             );

#endif