#ifndef __MONITORING_UTILS_H__
#define __MONITORING_UTILS_H__

#include "spdlog/spdlog.h"

// define monitoring threshold (in second)
const unsigned MONITORING_THRESHOLD = 30;

// define the grace period for triggering elasticity action (in second)
const unsigned GRACE_PERIOD = 120;

// the default number of nodes to add concurrently for storage
const unsigned NODE_ADD = 2;

// define capacity for both tiers
const double MEM_CAPACITY_MAX = 0.6;
const double MEM_CAPACITY_MIN = 0.3;
const double EBS_CAPACITY_MAX = 0.75;
const double EBS_CAPACITY_MIN = 0.5;

// define threshold for promotion/demotion
const unsigned PROMOTE_THRESHOLD = 0;
const unsigned DEMOTE_THRESHOLD = 1;

// define minimum number of nodes for each tier
const unsigned MINIMUM_MEMORY_NODE = 12;
const unsigned MINIMUM_EBS_NODE = 0;

// value size in KB
const unsigned VALUE_SIZE = 256;

struct SummaryStats {
  void clear() {
    key_access_mean = 0;
    key_access_std = 0;
    total_memory_access = 0;
    total_ebs_access = 0;
    total_memory_consumption = 0;
    total_ebs_consumption = 0;
    max_memory_consumption_percentage = 0;
    max_ebs_consumption_percentage = 0;
    avg_memory_consumption_percentage = 0;
    avg_ebs_consumption_percentage = 0;
    required_memory_node = 0;
    required_ebs_node = 0;
    max_memory_occupancy = 0;
    min_memory_occupancy = 1;
    avg_memory_occupancy = 0;
    max_ebs_occupancy = 0;
    min_ebs_occupancy = 1;
    avg_ebs_occupancy = 0;
    min_occupancy_memory_ip = Address();
    avg_latency = 0;
    total_throughput = 0;
  }
  SummaryStats() { clear(); }
  double key_access_mean;
  double key_access_std;
  unsigned total_memory_access;
  unsigned total_ebs_access;
  unsigned long long total_memory_consumption;
  unsigned long long total_ebs_consumption;
  double max_memory_consumption_percentage;
  double max_ebs_consumption_percentage;
  double avg_memory_consumption_percentage;
  double avg_ebs_consumption_percentage;
  unsigned required_memory_node;
  unsigned required_ebs_node;
  double max_memory_occupancy;
  double min_memory_occupancy;
  double avg_memory_occupancy;
  double max_ebs_occupancy;
  double min_ebs_occupancy;
  double avg_ebs_occupancy;
  Address min_occupancy_memory_ip;
  double avg_latency;
  double total_throughput;
};

Address prepare_metadata_request(
    const std::string& key, GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring,
    std::unordered_map<Address, communication::Request>& addr_request_map,
    MonitoringThread& mt, unsigned& rid, std::string type);

void prepare_metadata_get_request(
    const std::string& key, GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring,
    std::unordered_map<Address, communication::Request>& addr_request_map,
    MonitoringThread& mt, unsigned& rid);

void prepare_metadata_put_request(
    const std::string& key, const std::string& value,
    GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring,
    std::unordered_map<Address, communication::Request>& addr_request_map,
    MonitoringThread& mt, unsigned& rid);

void collect_internal_stats(
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    SocketCache& pushers, MonitoringThread& mt, zmq::socket_t& response_puller,
    std::shared_ptr<spdlog::logger> logger, unsigned& rid,
    std::unordered_map<std::string, std::unordered_map<Address, unsigned>>&
        key_access_frequency,
    std::unordered_map<Address,
                       std::unordered_map<unsigned, unsigned long long>>&
        memory_tier_storage,
    std::unordered_map<Address,
                       std::unordered_map<unsigned, unsigned long long>>&
        ebs_tier_storage,
    std::unordered_map<
        Address, std::unordered_map<unsigned, std::pair<double, unsigned>>>&
        memory_tier_occupancy,
    std::unordered_map<
        Address, std::unordered_map<unsigned, std::pair<double, unsigned>>>&
        ebs_tier_occupancy,
    std::unordered_map<Address, std::unordered_map<unsigned, unsigned>>&
        memory_tier_access,
    std::unordered_map<Address, std::unordered_map<unsigned, unsigned>>&
        ebs_tier_access,
    std::unordered_map<unsigned, TierData>& tier_data_map);

void compute_summary_stats(
    std::unordered_map<std::string, std::unordered_map<Address, unsigned>>&
        key_access_frequency,
    std::unordered_map<Address,
                       std::unordered_map<unsigned, unsigned long long>>&
        memory_tier_storage,
    std::unordered_map<Address,
                       std::unordered_map<unsigned, unsigned long long>>&
        ebs_tier_storage,
    std::unordered_map<
        Address, std::unordered_map<unsigned, std::pair<double, unsigned>>>&
        memory_tier_occupancy,
    std::unordered_map<
        Address, std::unordered_map<unsigned, std::pair<double, unsigned>>>&
        ebs_tier_occupancy,
    std::unordered_map<Address, std::unordered_map<unsigned, unsigned>>&
        memory_tier_access,
    std::unordered_map<Address, std::unordered_map<unsigned, unsigned>>&
        ebs_tier_access,
    std::unordered_map<std::string, unsigned>& key_access_summary,
    SummaryStats& ss, std::shared_ptr<spdlog::logger> logger,
    unsigned& server_monitoring_epoch,
    std::unordered_map<unsigned, TierData>& tier_data_map);

void collect_external_stats(
    std::unordered_map<std::string, double>& user_latency,
    std::unordered_map<std::string, double>& user_throughput, SummaryStats& ss,
    std::shared_ptr<spdlog::logger> logger);

KeyInfo create_new_replication_vector(unsigned gm, unsigned ge, unsigned lm,
                                      unsigned le);

void prepare_replication_factor_update(
    const std::string& key,
    std::unordered_map<Address, communication::Replication_Factor_Request>&
        replication_factor_map,
    Address server_address,
    std::unordered_map<std::string, KeyInfo>& placement);

void change_replication_factor(
    std::unordered_map<std::string, KeyInfo>& requests,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::vector<Address>& routing_address,
    std::unordered_map<std::string, KeyInfo>& placement, SocketCache& pushers,
    MonitoringThread& mt, zmq::socket_t& response_puller,
    std::shared_ptr<spdlog::logger> logger, unsigned& rid);

void add_node(std::shared_ptr<spdlog::logger> logger, std::string tier,
              unsigned number, unsigned& adding,
              const Address& management_address);

void remove_node(std::shared_ptr<spdlog::logger> logger, ServerThread& node,
                 std::string tier, bool& removing_flag, SocketCache& pushers,
                 std::unordered_map<Address, unsigned>& departing_node_map,
                 MonitoringThread& mt,
                 std::unordered_map<unsigned, TierData>& tier_data_map);

#endif
