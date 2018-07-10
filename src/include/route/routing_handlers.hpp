#ifndef __ROUTING_HANDLERS_H__
#define __ROUTING_HANDLERS_H__

#include "hash_ring.hpp"
#include "spdlog/spdlog.h"

void seed_handler(
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* addr_responder,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unsigned long long duration);

void membership_handler(
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* notify_puller,
    SocketCache& pushers,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unsigned thread_id, Address ip);

void replication_response_handler(
    std::shared_ptr<spdlog::logger> logger,
    zmq::socket_t* replication_factor_puller, SocketCache& pushers,
    RoutingThread& rt,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, KeyInfo>& placement,
    PendingMap<std::pair<Address, std::string>>& pending_key_request_map,
    unsigned& seed);

void replication_change_handler(
    std::shared_ptr<spdlog::logger> logger,
    zmq::socket_t* replication_factor_change_puller, SocketCache& pushers,
    std::unordered_map<Key, KeyInfo>& placement, unsigned thread_id,
    Address ip);

void address_handler(
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* key_address_puller,
    SocketCache& pushers, RoutingThread& rt,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, KeyInfo>& placement,
    PendingMap<std::pair<Address, std::string>>& pending_key_request_map,
    unsigned& seed);

#endif
