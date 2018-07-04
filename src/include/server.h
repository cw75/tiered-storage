#ifndef __SERVER_H__
#define __SERVER_H__

#include "utils/server_utility.h"
void join_handler(unsigned int self_tier_id,
             unsigned int thread_num,
             string ip,
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
             );
