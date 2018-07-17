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

#ifndef TESTS_MOCKED_HPP_
#define TESTS_MOCKED_HPP_

#include "hash_ring.hpp"
#include "zmq/zmq_util.hpp"

class MockZmqUtil : public ZmqUtilInterface {
 public:
  virtual std::string message_to_string(const zmq::message_t& message);
  virtual zmq::message_t string_to_message(const std::string& s);
  virtual void send_string(const std::string& s, zmq::socket_t* socket);
  virtual std::string recv_string(zmq::socket_t* socket);
  virtual int poll(long timeout, std::vector<zmq::pollitem_t>* items);
};

class MockHashRingUtil : public HashRingUtilInterface {
 public:
  virtual ServerThreadSet get_responsible_threads(
      Address respond_address, const Key& key, bool metadata,
      std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
      std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
      std::unordered_map<Key, KeyInfo>& placement, SocketCache& pushers,
      const std::vector<unsigned>& tier_ids, bool& succeed, unsigned& seed);

  virtual ServerThreadSet responsible_global(const Key& key,
                                             unsigned global_rep,
                                             GlobalHashRing& global_hash_ring);

  virtual std::unordered_set<unsigned> responsible_local(
      const Key& key, unsigned local_rep, LocalHashRing& local_hash_ring);

  virtual ServerThreadSet get_responsible_threads_metadata(
      const Key& key, GlobalHashRing& global_memory_hash_ring,
      LocalHashRing& local_memory_hash_ring);

  virtual void issue_replication_factor_request(
      const Address& respond_address, const Key& key,
      GlobalHashRing& global_memory_hash_ring,
      LocalHashRing& local_memory_hash_ring, SocketCache& pushers,
      unsigned& seed);

  virtual std::vector<Address> get_address_from_routing(
      UserThread& ut, const Key& key, zmq::socket_t& sending_socket,
      zmq::socket_t& receiving_socket, bool& succeed, Address& ip,
      unsigned& thread_id, unsigned& rid);

  virtual RoutingThread get_random_routing_thread(
      std::vector<Address>& routing_address, unsigned& seed,
      unsigned& kRoutingThreadCount);
};

#endif  // TESTS_MOCKED_HPP_