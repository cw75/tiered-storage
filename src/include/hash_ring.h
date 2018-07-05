#ifndef __HASH_RING_H__
#define __HASH_RING_H__

#include "threads.h"
#include "hashers.h"

typedef consistent_hash_map<server_thread_t, global_hasher> global_hash_t;
typedef consistent_hash_map<server_thread_t, local_hasher> local_hash_t;

template<typename H>
bool insert_to_hash_ring(H& hash_ring, std::string ip, unsigned tid) {
  bool succeed;

  for (unsigned virtual_num = 0; virtual_num < VIRTUAL_THREAD_NUM; virtual_num++) {
    succeed = hash_ring.insert(server_thread_t(ip, tid, virtual_num)).second;
  }

  return succeed;
}

template<typename H>
void remove_from_hash_ring(H& hash_ring, std::string ip, unsigned tid) {
  for (unsigned virtual_num = 0; virtual_num < VIRTUAL_THREAD_NUM; virtual_num++) {
    hash_ring.erase(server_thread_t(ip, tid, virtual_num));
  }
}

bool operator==(const server_thread_t& l, const server_thread_t& r);

unordered_set<server_thread_t, thread_hash> responsible_global
    (string key, unsigned global_rep, global_hash_t& global_hash_ring);

unordered_set<unsigned> responsible_local
    (string key, unsigned local_rep, local_hash_t& local_hash_ring);

unordered_set<server_thread_t, thread_hash> get_responsible_threads_metadata(
    string& key,
    global_hash_t& global_memory_hash_ring,
    local_hash_t& local_memory_hash_ring);

unordered_set<server_thread_t, thread_hash> get_responsible_threads(
    string respond_address,
    string key,
    bool metadata,
    unordered_map<unsigned, global_hash_t>& global_hash_ring_map,
    unordered_map<unsigned, local_hash_t>& local_hash_ring_map,
    unordered_map<string, key_info>& placement,
    SocketCache& pushers,
    vector<unsigned>& tier_ids,
    bool& succeed,
    unsigned& seed);

void issue_replication_factor_request(
    const std::string& respond_address,
    const std::string& key,
    global_hash_t& global_memory_hash_ring,
    local_hash_t& local_memory_hash_ring,
    SocketCache& pushers,
    unsigned& seed);

vector<std::string> get_address_from_routing(
    user_thread_t& ut,
    std::string key,
    zmq::socket_t& sending_socket,
    zmq::socket_t& receiving_socket,
    bool& succeed,
    std::string& ip,
    unsigned& thread_id,
    unsigned& rid);

routing_thread_t get_random_routing_thread
    (std::vector<std::string>& routing_address, unsigned& seed);

void warmup(std::unordered_map<std::string, key_info>& placement);

#endif
