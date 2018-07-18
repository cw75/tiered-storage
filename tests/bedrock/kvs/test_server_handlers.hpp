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

#include <stdio.h>
#include <stdlib.h>

#include <iostream>

#include "gtest/gtest.h"
#include "kvs/kvs_handlers.hpp"
#include "mocked.hpp"

std::shared_ptr<spdlog::logger> logger =
    spdlog::basic_logger_mt("mock_logger", "mock.txt", true);

class ServerHandlerTest : public ::testing::Test {
 protected:
  Address ip = "127.0.0.1";
  unsigned thread_id = 0;
  std::unordered_map<unsigned, GlobalHashRing> global_hash_ring_map;
  std::unordered_map<unsigned, LocalHashRing> local_hash_ring_map;
  std::unordered_map<Key, KeyInfo> placement;
  std::unordered_map<Key, unsigned> key_size_map;
  ServerThread wt;
  zmq::context_t context;
  SocketCache pushers = SocketCache(&context, ZMQ_PUSH);
  Serializer* serializer;
  MemoryKVS* kvs;
  ServerHandlerTest() {
    kvs = new MemoryKVS();
    serializer = new MemorySerializer(kvs);
    wt = ServerThread(ip, thread_id);
    global_hash_ring_map[1].insert(ip, thread_id);
  }
  virtual ~ServerHandlerTest() {
    delete kvs;
    delete serializer;
  }
};

TEST_F(ServerHandlerTest, NodeJoin) {
  unsigned seed = 0;
  std::unordered_set<Key> join_remove_set;
  AddressKeysetMap join_addr_keyset_map;

  EXPECT_EQ(global_hash_ring_map[1].size(), 3000);
  std::string serialized = "1:1.0.0.1";
  node_join_handler(thread_id, seed, ip, logger, serialized,
                    global_hash_ring_map, local_hash_ring_map, key_size_map,
                    placement, join_remove_set, pushers, wt,
                    join_addr_keyset_map);

  EXPECT_EQ(global_hash_ring_map[1].size(), 6000);
}

TEST_F(ServerHandlerTest, NodeDepart) {
  global_hash_ring_map[1].insert("1.0.0.1", 0);

  EXPECT_EQ(global_hash_ring_map[1].size(), 6000);
  std::string serialized = "1:1.0.0.1";
  node_depart_handler(thread_id, ip, global_hash_ring_map, logger, serialized,
                      pushers);

  EXPECT_EQ(global_hash_ring_map[1].size(), 3000);
}

TEST_F(ServerHandlerTest, SelfDepart) {
  unsigned seed = 0;
  std::vector<Address> routing_address;
  std::vector<Address> monitoring_address;

  EXPECT_EQ(global_hash_ring_map[1].size(), 3000);
  std::string serialized = "tcp://1.0.0.1:6560";
  self_depart_handler(thread_id, seed, ip, logger, serialized,
                      global_hash_ring_map, local_hash_ring_map, key_size_map,
                      placement, routing_address, monitoring_address, wt,
                      pushers, serializer);

  EXPECT_EQ(global_hash_ring_map[1].size(), 0);
}
