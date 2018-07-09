#include "hash_ring.hpp"

#include <unistd.h>

#include <atomic>
#include <functional>
#include <string>

#include "common.hpp"
#include "communication.pb.h"
#include "requests.hpp"
#include "spdlog/spdlog.h"
#include "threads.hpp"
#include "yaml-cpp/yaml.h"
#include "zmq/socket_cache.hpp"
#include "zmq/zmq_util.hpp"

// assuming the replication factor will never be greater than the number of
// nodes in a tier return a set of ServerThread that are responsible for a key
std::unordered_set<ServerThread, ThreadHash> responsible_global(
    const std::string& key, unsigned global_rep,
    GlobalHashRing& global_hash_ring) {
  std::unordered_set<ServerThread, ThreadHash> threads;
  auto pos = global_hash_ring.find(key);

  if (pos != global_hash_ring.end()) {
    // iterate for every value in the replication factor
    unsigned i = 0;

    while (i < global_rep &&
           i != global_hash_ring.size() / kVirtualThreadNum) {
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

// assuming the replication factor will never be greater than the number of
// worker threads return a set of tids that are responsible for a key
std::unordered_set<unsigned> responsible_local(const std::string& key,
                                               unsigned local_rep,
                                               LocalHashRing& local_hash_ring) {
  std::unordered_set<unsigned> tids;
  auto pos = local_hash_ring.find(key);

  if (pos != local_hash_ring.end()) {
    // iterate for every value in the replication factor
    unsigned i = 0;

    while (i < local_rep && i != local_hash_ring.size() / kVirtualThreadNum) {
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

std::unordered_set<ServerThread, ThreadHash> get_responsible_threads_metadata(
    const std::string& key, GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring) {
  std::unordered_set<ServerThread, ThreadHash> threads;
  auto mts = responsible_global(key, kMetadataReplicationFactor,
                                global_memory_hash_ring);

  for (auto it = mts.begin(); it != mts.end(); it++) {
    std::string ip = it->get_ip();
    auto tids = responsible_local(key, kDefaultLocalReplication,
                                  local_memory_hash_ring);

    for (auto iter = tids.begin(); iter != tids.end(); iter++) {
      threads.insert(ServerThread(ip, *iter));
    }
  }

  return threads;
}

void issue_replication_factor_request(const std::string& respond_address,
                                      const std::string& key,
                                      GlobalHashRing& global_memory_hash_ring,
                                      LocalHashRing& local_memory_hash_ring,
                                      SocketCache& pushers, unsigned& seed) {
  std::string key_rep =
      std::string(kMetadataIdentifier) + "_" + key + "_replication";
  auto threads = get_responsible_threads_metadata(
      key_rep, global_memory_hash_ring, local_memory_hash_ring);

  std::string target_address =
      next(begin(threads), rand_r(&seed) % threads.size())
          ->get_request_pulling_connect_addr();

  communication::Request req;
  req.set_type("GET");
  req.set_respond_address(respond_address);

  prepare_get_tuple(
      req, std::string(kMetadataIdentifier) + "_" + key + "_replication");
  push_request(req, pushers[target_address]);
}

// get all threads responsible for a key from the "node_type" tier
// metadata flag = 0 means the key is  metadata; otherwise, it is  regular data
std::unordered_set<ServerThread, ThreadHash> get_responsible_threads(
    std::string respond_address, const std::string& key, bool metadata,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<std::string, KeyInfo>& placement, SocketCache& pushers,
    std::vector<unsigned>& tier_ids, bool& succeed, unsigned& seed) {
  if (metadata) {
    succeed = true;
    return get_responsible_threads_metadata(key, global_hash_ring_map[1],
                                            local_hash_ring_map[1]);
  } else {
    std::unordered_set<ServerThread, ThreadHash> result;

    if (placement.find(key) == placement.end()) {
      issue_replication_factor_request(respond_address, key,
                                       global_hash_ring_map[1],
                                       local_hash_ring_map[1], pushers, seed);
      succeed = false;
    } else {
      for (auto id_iter = tier_ids.begin(); id_iter != tier_ids.end();
           id_iter++) {
        unsigned tier_id = *id_iter;
        auto mts = responsible_global(
            key, placement[key].global_replication_map_[tier_id],
            global_hash_ring_map[tier_id]);

        for (auto it = mts.begin(); it != mts.end(); it++) {
          std::string ip = it->get_ip();
          auto tids = responsible_local(
              key, placement[key].local_replication_map_[tier_id],
              local_hash_ring_map[tier_id]);

          for (auto iter = tids.begin(); iter != tids.end(); iter++) {
            result.insert(ServerThread(ip, *iter));
          }
        }
      }

      succeed = true;
    }

    return result;
  }
}

// query the routing for a key and return all address
std::vector<std::string> get_address_from_routing(
    UserThread& ut, const std::string& key, zmq::socket_t& sending_socket,
    zmq::socket_t& receiving_socket, bool& succeed, std::string& ip,
    unsigned& thread_id, unsigned& rid) {
  int count = 0;

  communication::Key_Request key_req;
  communication::Key_Response key_response;
  key_req.set_respond_address(ut.get_key_address_connect_addr());
  key_req.add_keys(key);

  std::string req_id =
      ip + ":" + std::to_string(thread_id) + "_" + std::to_string(rid);
  key_req.set_request_id(req_id);
  std::vector<std::string> result;

  int err_number = -1;

  while (err_number != 0) {
    if (err_number == 1) {
      std::cerr << "No servers have joined the cluster yet. Retrying request."
                << std::endl;
    }

    if (count > 0 && count % 5 == 0) {
      std::cerr
          << "Pausing for 5 seconds before continuing to query routing layer..."
          << std::endl;
      usleep(5000000);
    }

    rid += 1;

    // query routing for addresses on the other tier
    key_response =
        send_request<communication::Key_Request, communication::Key_Response>(
            key_req, sending_socket, receiving_socket, succeed);

    if (!succeed) {
      return result;
    } else {
      err_number = key_response.err_number();
    }

    count++;
  }

  for (int j = 0; j < key_response.tuple(0).addresses_size(); j++) {
    result.push_back(key_response.tuple(0).addresses(j));
  }

  return result;
}

RoutingThread get_random_routing_thread(
    std::vector<std::string>& routing_address, unsigned& seed,
    unsigned& kRoutingThreadCount) {
  std::string routing_ip =
      routing_address[rand_r(&seed) % routing_address.size()];
  unsigned tid = rand_r(&seed) % kRoutingThreadCount;
  return RoutingThread(routing_ip, tid);
}
