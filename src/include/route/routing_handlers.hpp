#ifndef __ROUTING_HANDLERS_H__
#define __ROUTING_HANDLERS_H__

#include "spdlog/spdlog.h"

using namespace std;

void seed_handler(std::shared_ptr<spdlog::logger> logger,
                  zmq::socket_t* addr_responder,
                  unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
                  unsigned long long duration);

void membership_handler(
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* notify_puller,
    SocketCache& pushers,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unsigned thread_id, string ip);

void replication_response_handler(
    std::shared_ptr<spdlog::logger> logger,
    zmq::socket_t* replication_factor_puller, SocketCache& pushers,
    RoutingThread& rt, unordered_map<unsigned, TierData>& tier_data_map,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    unordered_map<string, KeyInfo>& placement,
    unordered_map<string, pair<chrono::system_clock::time_point,
                               vector<pair<string, string>>>>&
        pending_key_request_map,
    unsigned& seed);

void replication_change_handler(std::shared_ptr<spdlog::logger> logger,
                                zmq::socket_t* replication_factor_change_puller,
                                SocketCache& pushers,
                                unordered_map<string, KeyInfo>& placement,
                                unsigned thread_id, string ip);

void address_handler(
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* key_address_puller,
    SocketCache& pushers, RoutingThread& rt,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    unordered_map<string, KeyInfo>& placement,
    unordered_map<string, pair<chrono::system_clock::time_point,
                               vector<pair<string, string>>>>&
        pending_key_request_map,
    unsigned& seed);

#endif
