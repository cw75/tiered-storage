#include <atomic>
#include <string>
#include <functional>
#include "communication.pb.h"
#include "zmq/socket_cache.h"
#include "zmq/zmq_util.h"

#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"

#include "common.h"
#include "threads.h"
#include "requests.h"
#include "hash_ring.h"

// assuming the replication factor will never be greater than the number of nodes in a tier
// return a set of server_thread_t that are responsible for a key
unordered_set<server_thread_t, thread_hash> responsible_global(string key, unsigned global_rep, global_hash_t& global_hash_ring) {
  unordered_set<server_thread_t, thread_hash> threads;
  auto pos = global_hash_ring.find(key);

  if (pos != global_hash_ring.end()) {
    // iterate for every value in the replication factor
    unsigned i = 0;

    while (i < global_rep && i != global_hash_ring.size() / VIRTUAL_THREAD_NUM) {
      bool succeed = threads.insert(pos->second).second;
      if (++pos == global_hash_ring.end()) {
        pos = global_hash_ring.begin();
      }

      if (succeed) {
        i += 1;
      }
    }
  }

  return threads;
}

// assuming the replication factor will never be greater than the number of worker threads
// return a set of tids that are responsible for a key
unordered_set<unsigned> responsible_local(string key, unsigned local_rep, local_hash_t& local_hash_ring) {
  unordered_set<unsigned> tids;
  auto pos = local_hash_ring.find(key);

  if (pos != local_hash_ring.end()) {
    // iterate for every value in the replication factor
    unsigned i = 0;

    while (i < local_rep && i != local_hash_ring.size() / VIRTUAL_THREAD_NUM) {
      bool succeed = tids.insert(pos->second.get_tid()).second;
      if (++pos == local_hash_ring.end()) {
        pos = local_hash_ring.begin();
      }

      if (succeed) {
        i += 1;
      }
    }
  }

  return tids;
}

unordered_set<server_thread_t, thread_hash> get_responsible_threads_metadata(
    string& key,
    global_hash_t& global_memory_hash_ring,
    local_hash_t& local_memory_hash_ring) {

  unordered_set<server_thread_t, thread_hash> threads;
  auto mts = responsible_global(key, METADATA_REPLICATION_FACTOR, global_memory_hash_ring);

  for (auto it = mts.begin(); it != mts.end(); it++) {
    string ip = it->get_ip();
    auto tids = responsible_local(key, DEFAULT_LOCAL_REPLICATION, local_memory_hash_ring);

    for (auto iter = tids.begin(); iter != tids.end(); iter++) {
      threads.insert(server_thread_t(ip, *iter));
    }
  }

  return threads;
}

void issue_replication_factor_request(
    const string& respond_address,
    const string& key,
    global_hash_t& global_memory_hash_ring,
    local_hash_t& local_memory_hash_ring,
    SocketCache& pushers,

    unsigned& seed) {
  string key_rep = string(METADATA_IDENTIFIER) + "_" + key + "_replication";
  auto threads = get_responsible_threads_metadata(key_rep, global_memory_hash_ring, local_memory_hash_ring);

  if (threads.size() == 0) {
    cerr << "error!\n";
  }

  string target_address = next(begin(threads), rand_r(&seed) % threads.size())->get_request_pulling_connect_addr();

  communication::Request req;
  req.set_type("GET");
  req.set_respond_address(respond_address);

  prepare_get_tuple(req, string(METADATA_IDENTIFIER) + "_" + key + "_replication");
  push_request(req, pushers[target_address]);
}

// get all threads responsible for a key from the "node_type" tier
// metadata flag = 0 means the key is a metadata. Otherwise, it is a regular data
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
    unsigned& seed) {

  if (metadata) {
    succeed = true;
    return get_responsible_threads_metadata(key, global_hash_ring_map[1], local_hash_ring_map[1]);
  } else {
    unordered_set<server_thread_t, thread_hash> result;

    if (placement.find(key) == placement.end()) {
      issue_replication_factor_request(respond_address, key, global_hash_ring_map[1], local_hash_ring_map[1], pushers, seed);
      succeed = false;
    } else {
      for (auto id_iter = tier_ids.begin(); id_iter != tier_ids.end(); id_iter++) {
        unsigned tier_id = *id_iter;
        auto mts = responsible_global(key, placement[key].global_replication_map_[tier_id], global_hash_ring_map[tier_id]);

        for (auto it = mts.begin(); it != mts.end(); it++) {
          string ip = it->get_ip();
          auto tids = responsible_local(key, placement[key].local_replication_map_[tier_id], local_hash_ring_map[tier_id]);

          for (auto iter = tids.begin(); iter != tids.end(); iter++) {
            result.insert(server_thread_t(ip, *iter));
          }
        }
      }

      succeed = true;
    }

    return result;
  }
}


// query the routing for a key and return all address
vector<string> get_address_from_routing(
    user_thread_t& ut,
    string key,
    zmq::socket_t& sending_socket,
    zmq::socket_t& receiving_socket,
    bool& succeed,
    string& ip,
    unsigned& thread_id,
    unsigned& rid) {

  communication::Key_Request key_req;
  key_req.set_respond_address(ut.get_key_address_connect_addr());
  key_req.add_keys(key);

  string req_id = ip + ":" + to_string(thread_id) + "_" + to_string(rid);
  key_req.set_request_id(req_id);
  rid += 1;

  // query routing for addresses on the other tier
  auto key_response = send_request<communication::Key_Request, communication::Key_Response>(key_req, sending_socket, receiving_socket, succeed);
  vector<string> result;

  if (succeed) {
    for (int j = 0; j < key_response.tuple(0).addresses_size(); j++) {
      result.push_back(key_response.tuple(0).addresses(j));
    }
  }

  return result;
}

routing_thread_t get_random_routing_thread(vector<string>& routing_address, unsigned& seed) {
  string routing_ip = routing_address[rand_r(&seed) % routing_address.size()];
  unsigned tid = rand_r(&seed) % PROXY_THREAD_NUM;
  return routing_thread_t(routing_ip, tid);
}
