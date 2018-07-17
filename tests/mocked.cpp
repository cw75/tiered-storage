//  Copyright 2018 U.C. Berkeley RISE Lab
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#include "mocked.hpp"

std::string MockZmqUtil::message_to_string(const zmq::message_t& message) {
  return std::string(static_cast<const char*>(message.data()), message.size());
}
zmq::message_t MockZmqUtil::string_to_message(const std::string& s) {
  zmq::message_t msg(s.size());
  memcpy(msg.data(), s.c_str(), s.size());
  return msg;
}
void MockZmqUtil::send_string(const std::string& s, zmq::socket_t* socket) {}
std::string MockZmqUtil::recv_string(zmq::socket_t* socket) { return ""; }
int MockZmqUtil::poll(long timeout, std::vector<zmq::pollitem_t>* items) {
  return 0;
}

// get all threads responsible for a key from the "node_type" tier
// metadata flag = 0 means the key is  metadata; otherwise, it is  regular data
ServerThreadSet MockHashRingUtil::get_responsible_threads(
    Address respond_address, const Key& key, bool metadata,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, KeyInfo>& placement, SocketCache& pushers,
    const std::vector<unsigned>& tier_ids, bool& succeed, unsigned& seed) {
  ServerThreadSet threads;
  threads.insert(ServerThread("127.0.0.1", 0));
  return threads;
}

// assuming the replication factor will never be greater than the number of
// nodes in a tier return a set of ServerThreads that are responsible for a key
ServerThreadSet MockHashRingUtil::responsible_global(
    const Key& key, unsigned global_rep, GlobalHashRing& global_hash_ring) {
  ServerThreadSet threads;
  auto pos = global_hash_ring.find(key);

  if (pos != global_hash_ring.end()) {
    // iterate for every value in the replication factor
    unsigned i = 0;

    while (i < global_rep) {
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
std::unordered_set<unsigned> MockHashRingUtil::responsible_local(
    const Key& key, unsigned local_rep, LocalHashRing& local_hash_ring) {
  std::unordered_set<unsigned> tids;
  auto pos = local_hash_ring.find(key);

  if (pos != local_hash_ring.end()) {
    // iterate for every value in the replication factor
    unsigned i = 0;

    while (i < local_rep) {
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

ServerThreadSet MockHashRingUtil::get_responsible_threads_metadata(
    const Key& key, GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring) {
  ServerThreadSet threads = kHashRingUtilInterface->responsible_global(
      key, kMetadataReplicationFactor, global_memory_hash_ring);

  for (const ServerThread& thread : threads) {
    Address ip = thread.get_ip();
    std::unordered_set<unsigned> tids =
        kHashRingUtilInterface->responsible_local(key, kDefaultLocalReplication,
                                                  local_memory_hash_ring);

    for (const unsigned& tid : tids) {
      threads.insert(ServerThread(ip, tid));
    }
  }

  return threads;
}

void MockHashRingUtil::issue_replication_factor_request(
    const Address& respond_address, const Key& key,
    GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring, SocketCache& pushers,
    unsigned& seed) {
  Key replication_key = get_metadata_key(key, MetadataType::replication);
  auto threads = kHashRingUtilInterface->get_responsible_threads_metadata(
      replication_key, global_memory_hash_ring, local_memory_hash_ring);

  Address target_address = next(begin(threads), rand_r(&seed) % threads.size())
                               ->get_request_pulling_connect_addr();

  KeyRequest key_request;
  key_request.set_type(get_request_type("GET"));
  key_request.set_response_address(respond_address);

  prepare_get_tuple(key_request, replication_key);
  std::string serialized;
  key_request.SerializeToString(&serialized);
  kZmqUtilInterface->send_string(serialized, &pushers[target_address]);
}

// query the routing for a key and return all address
std::vector<Address> MockHashRingUtil::get_address_from_routing(
    UserThread& ut, const Key& key, zmq::socket_t& sending_socket,
    zmq::socket_t& receiving_socket, bool& succeed, Address& ip,
    unsigned& thread_id, unsigned& rid) {
  int count = 0;

  KeyAddressRequest address_request;
  KeyAddressResponse address_response;
  address_request.set_response_address(ut.get_key_address_connect_addr());
  address_request.add_keys(key);

  std::string req_id =
      ip + ":" + std::to_string(thread_id) + "_" + std::to_string(rid);
  address_request.set_request_id(req_id);
  std::vector<Address> result;

  return result;
}

RoutingThread MockHashRingUtil::get_random_routing_thread(
    std::vector<Address>& routing_address, unsigned& seed,
    unsigned& kRoutingThreadCount) {
  Address routing_ip = routing_address[rand_r(&seed) % routing_address.size()];
  unsigned tid = rand_r(&seed) % kRoutingThreadCount;
  return RoutingThread(routing_ip, tid);
}
