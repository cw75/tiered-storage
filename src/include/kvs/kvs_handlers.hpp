#ifndef __KVS_HANDLERS_H__
#define __KVS_HANDLERS_H__

#include "spdlog/spdlog.h"
#include "utils/server_utility.hpp"

void node_join_handler(
    unsigned int thread_num, unsigned thread_id, unsigned& seed, string ip,
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* join_puller,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    unordered_map<string, KeyStat>& key_stat_map,
    unordered_map<string, KeyInfo>& placement,
    unordered_set<string>& join_remove_set, SocketCache& pushers,
    ServerThread& wt, AddressKeysetMap& join_addr_keyset_map);

void node_depart_handler(
    unsigned int thread_num, unsigned thread_id, string ip,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* depart_puller,
    SocketCache& pushers);

void self_depart_handler(
    unsigned thread_num, unsigned thread_id, unsigned& seed, string ip,
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* self_depart_puller,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    unordered_map<string, KeyStat>& key_stat_map,
    unordered_map<string, KeyInfo>& placement, vector<string> routing_address,
    vector<string> monitoring_address, ServerThread wt, SocketCache& pushers,
    Serializer* serializer);

void user_request_handler(
    unsigned& total_access, unsigned& seed, zmq::socket_t* request_puller,
    chrono::system_clock::time_point& start_time,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    unordered_map<string, KeyStat>& key_stat_map,
    unordered_map<string, pair<chrono::system_clock::time_point,
                               vector<PendingRequest>>>& pending_request_map,
    unordered_map<string,
                  multiset<std::chrono::time_point<std::chrono::system_clock>>>&
        key_access_timestamp,
    unordered_map<string, KeyInfo>& placement,
    unordered_set<string>& local_changeset, ServerThread& wt,
    Serializer* serializer, SocketCache& pushers);

void gossip_handler(
    unsigned& seed, zmq::socket_t* gossip_puller,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    unordered_map<string, KeyStat>& key_stat_map,
    unordered_map<string, pair<chrono::system_clock::time_point,
                               vector<PendingGossip>>>& pending_gossip_map,
    unordered_map<string, KeyInfo>& placement, ServerThread& wt,
    Serializer* serializer, SocketCache& pushers);

void rep_factor_response_handler(
    unsigned& seed, unsigned& total_access,
    std::shared_ptr<spdlog::logger> logger,
    zmq::socket_t* rep_factor_response_puller,
    chrono::system_clock::time_point& start_time,
    unordered_map<unsigned, TierData> tier_data_map,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    unordered_map<string, pair<chrono::system_clock::time_point,
                               vector<PendingRequest>>>& pending_request_map,
    unordered_map<string,
                  pair<chrono::system_clock::time_point, vector<PendingGossip>>>
        pending_gossip_map,
    unordered_map<string,
                  multiset<std::chrono::time_point<std::chrono::system_clock>>>&
        key_access_timestamp,
    unordered_map<string, KeyInfo> placement,
    unordered_map<string, KeyStat>& key_stat_map,
    unordered_set<string>& local_changeset, ServerThread& wt,
    Serializer* serializer, SocketCache& pushers);

void rep_factor_change_handler(
    string ip, unsigned thread_id, unsigned thread_num, unsigned& seed,
    std::shared_ptr<spdlog::logger> logger,
    zmq::socket_t* rep_factor_change_puller,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    unordered_map<string, KeyInfo> placement,
    unordered_map<string, KeyStat>& key_stat_map,
    unordered_set<string>& local_changeset, ServerThread& wt,
    Serializer* serializer, SocketCache& pushers);

void send_gossip(AddressKeysetMap& addr_keyset_map, SocketCache& pushers,
                 Serializer* serializer);

pair<ReadCommittedPairLattice<string>, unsigned> process_get(
    const string& key, Serializer* serializer);

void process_put(const string& key, const unsigned long long& timestamp,
                 const string& value, Serializer* serializer,
                 unordered_map<string, KeyStat>& key_stat_map);
#endif
