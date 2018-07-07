#ifndef __POLICY_ALGORITHM_H__
#define __POLICY_ALGORITHM_H__

using namespace std;

void
policy_algorithm(shared_ptr<spdlog::logger> logger,
                 unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
                 unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
                 chrono::time_point<chrono::system_clock>& grace_start,
                 SummaryStats& ss,
                 unsigned& adding_memory_node,
                 unsigned& adding_ebs_node,
                 bool& removing_memory_node,
                 bool& removing_ebs_node,
                 string management_address,
                 unordered_map<string, KeyInfo>& placement,
                 unordered_map<string, unsigned>& key_access_summary,
                 MonitoringThread& mt,
                 unordered_map<unsigned, TierData>& tier_data_map,
                 unordered_map<string, unsigned>& departing_node_map,
                 SocketCache& pushers,
                 zmq::socket_t& response_puller,
                 vector<string>& routing_address,
                 unsigned& rid,
                 unordered_map<string, pair<double, unsigned>>& rep_factor_map,
                 const unsigned MINIMUM_MEMORY_NODE,
                 const unsigned MINIMUM_EBS_NODE,
                 const unsigned PROMOTE_THRESHOLD,
                 const unsigned DEMOTE_THRESHOLD,
                 const double MEM_CAPACITY_MAX,
                 const double EBS_CAPACITY_MAX,
                 const double EBS_CAPACITY_MIN,
                 const unsigned VALUE_SIZE,
                 const unsigned VIRTUAL_THREAD_NUM,
                 const unsigned GRACE_PERIOD,
                 const unsigned NODE_ADD
                 );

#endif