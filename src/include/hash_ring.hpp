#ifndef __HASH_RING_H__
#define __HASH_RING_H__

#include "hashers.hpp"
#include "threads.hpp"

typedef ConsistentHashMap<ServerThread, GlobalHasher> GlobalHashRing;
typedef ConsistentHashMap<ServerThread, LocalHasher> LocalHashRing;

template<typename H>
bool insert_to_hash_ring(H& hash_ring, std::string ip, unsigned tid) {
  bool succeed;

  for (unsigned virtual_num = 0; virtual_num < VIRTUAL_THREAD_NUM; virtual_num++) {
    succeed = hash_ring.insert(ServerThread(ip, tid, virtual_num)).second;
  }

  return succeed;
}

template<typename H>
void remove_from_hash_ring(H& hash_ring, std::string ip, unsigned tid) {
  for (unsigned virtual_num = 0; virtual_num < VIRTUAL_THREAD_NUM; virtual_num++) {
    hash_ring.erase(ServerThread(ip, tid, virtual_num));
  }
}

unordered_set<ServerThread, ThreadHash> responsible_global
    (string key, unsigned global_rep, GlobalHashRing& global_hash_ring);

unordered_set<unsigned> responsible_local
    (string key, unsigned local_rep, LocalHashRing& local_hash_ring);

unordered_set<ServerThread, ThreadHash> get_responsible_threads_metadata(
    string& key,
    GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring);

unordered_set<ServerThread, ThreadHash> get_responsible_threads(
    string respond_address,
    string key,
    bool metadata,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    unordered_map<string, KeyInfo>& placement,
    SocketCache& pushers,
    vector<unsigned>& tier_ids,
    bool& succeed,
    unsigned seed);

void issue_replication_factor_request(
    const std::string& respond_address,
    const std::string& key,
    GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring,
    SocketCache& pushers,
    unsigned seed);

vector<std::string> get_address_from_routing(
    UserThread& ut,
    std::string key,
    zmq::socket_t& sending_socket,
    zmq::socket_t& receiving_socket,
    bool& succeed,
    std::string& ip,
    unsigned& thread_id,
    unsigned& rid);

RoutingThread get_random_routing_thread
    (std::vector<std::string>& routing_address, unsigned& seed, unsigned& ROUTING_THREAD_NUM);

inline void warmup_placement_to_defaults(unordered_map<string, 
    KeyInfo>& placement,
    unsigned& DEFAULT_GLOBAL_MEMORY_REPLICATION,
    unsigned& DEFAULT_GLOBAL_EBS_REPLICATION,
    unsigned &DEFAULT_LOCAL_REPLICATION) {
  for (unsigned i = 1; i <= 1000000; i++) {
    // key is 8 bytes
    string key = string(8 - to_string(i).length(), '0') + to_string(i);
    placement[key].global_replication_map_[1] = DEFAULT_GLOBAL_MEMORY_REPLICATION;
    placement[key].global_replication_map_[2] = DEFAULT_GLOBAL_EBS_REPLICATION;
    placement[key].local_replication_map_[1] = DEFAULT_LOCAL_REPLICATION;
    placement[key].local_replication_map_[2] = DEFAULT_LOCAL_REPLICATION;
  }
}
#endif
