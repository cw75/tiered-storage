#ifndef __POLICIES_H__
#define __POLICIES_H__

#include "hash_ring.hpp"
#include "spdlog/spdlog.h"

void storage_policy(
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::chrono::time_point<std::chrono::system_clock>& grace_start,
    SummaryStats& ss, unsigned& memory_node_number, unsigned& ebs_node_number,
    unsigned& adding_memory_node, unsigned& adding_ebs_node,
    bool& removing_ebs_node, Address management_address, MonitoringThread& mt,
    std::unordered_map<Address, unsigned>& departing_node_map,
    SocketCache& pushers);

void movement_policy(
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::chrono::time_point<std::chrono::system_clock>& grace_start,
    SummaryStats& ss, unsigned& memory_node_number, unsigned& ebs_node_number,
    unsigned& adding_memory_node, unsigned& adding_ebs_node,
    Address management_address, std::unordered_map<Key, KeyInfo>& placement,
    std::unordered_map<Key, unsigned>& key_access_summary,
    std::unordered_map<Key, unsigned>& key_size, MonitoringThread& mt,
    SocketCache& pushers,
    zmq::socket_t& response_puller, std::vector<Address>& routing_address,
    unsigned& rid);

void slo_policy(
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::chrono::time_point<std::chrono::system_clock>& grace_start,
    SummaryStats& ss, unsigned& memory_node_number,
    unsigned& adding_memory_node, bool& removing_memory_node,
    Address management_address, std::unordered_map<Key, KeyInfo>& placement,
    std::unordered_map<Key, unsigned>& key_access_summary, MonitoringThread& mt,
    std::unordered_map<Address, unsigned>& departing_node_map,
    SocketCache& pushers, zmq::socket_t& response_puller,
    std::vector<Address>& routing_address, unsigned& rid,
    std::unordered_map<Key, std::pair<double, unsigned>>& latency_miss_ratio_map);

#endif
