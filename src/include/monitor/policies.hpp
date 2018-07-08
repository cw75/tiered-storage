#ifndef __POLICIES_H__
#define __POLICIES_H__

void
storage_policy(shared_ptr<spdlog::logger> logger,
               unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
               chrono::time_point<chrono::system_clock>& grace_start,
               SummaryStats& ss,
               unsigned& memory_node_number,
               unsigned& ebs_node_number,
               unsigned& adding_memory_node,
               unsigned& adding_ebs_node,
               bool& removing_ebs_node,
               string management_address,
               MonitoringThread& mt,
               unordered_map<unsigned, TierData>& tier_data_map,
               unordered_map<string, unsigned>& departing_node_map,
               SocketCache& pushers
               );

void
movement_policy(shared_ptr<spdlog::logger> logger,
                unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
                unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
                chrono::time_point<chrono::system_clock>& grace_start,
                SummaryStats& ss,
                unsigned& memory_node_number,
                unsigned& ebs_node_number,
                unsigned& adding_memory_node,
                unsigned& adding_ebs_node,
                string management_address,
                unordered_map<string, KeyInfo>& placement,
                unordered_map<string, unsigned>& key_access_summary,
                MonitoringThread& mt,
                unordered_map<unsigned, TierData>& tier_data_map,
                SocketCache& pushers,
                zmq::socket_t& response_puller,
                vector<string>& routing_address,
                unsigned& rid
                );

void
slo_policy(shared_ptr<spdlog::logger> logger,
           unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
           unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
           chrono::time_point<chrono::system_clock>& grace_start,
           SummaryStats& ss,
           unsigned& memory_node_number,
           unsigned& adding_memory_node,
           bool& removing_memory_node,
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
           unordered_map<string, pair<double, unsigned>>& rep_factor_map
           );

#endif