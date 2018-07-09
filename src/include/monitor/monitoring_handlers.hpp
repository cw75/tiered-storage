#ifndef __MONITORING_HANDLERS_H__
#define __MONITORING_HANDLERS_H__

#include "spdlog/spdlog.h"

using namespace std;

void membership_handler(
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* notify_puller,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unsigned& adding_memory_node, unsigned& adding_ebs_node,
    chrono::time_point<chrono::system_clock>& grace_start,
    vector<string>& routing_address,
    unordered_map<string, unordered_map<unsigned, unsigned long long>>&
        memory_tier_storage,
    unordered_map<string, unordered_map<unsigned, unsigned long long>>&
        ebs_tier_storage,
    unordered_map<string, unordered_map<unsigned, pair<double, unsigned>>>&
        memory_tier_occupancy,
    unordered_map<string, unordered_map<unsigned, pair<double, unsigned>>>&
        ebs_tier_occupancy,
    unordered_map<string, unordered_map<string, unsigned>>&
        key_access_frequency);

void depart_done_handler(std::shared_ptr<spdlog::logger> logger,
                         zmq::socket_t* depart_done_puller,
                         unordered_map<string, unsigned>& departing_node_map,
                         string management_address, bool& removing_memory_node,
                         bool& removing_ebs_node,
                         chrono::time_point<chrono::system_clock>& grace_start);

void feedback_handler(
    zmq::socket_t* feedback_puller, unordered_map<string, double>& user_latency,
    unordered_map<string, double>& user_throughput,
    unordered_map<string, pair<double, unsigned>>& rep_factor_map);

#endif
