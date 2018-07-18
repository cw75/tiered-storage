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
#include "route/routing_handlers.hpp"

std::shared_ptr<spdlog::logger> logger =
    spdlog::basic_logger_mt("mock_logger", "mock.txt", true);

class RoutingHandlerTest : public ::testing::Test {
 protected:
  Address ip = "127.0.0.1";
  unsigned thread_id = 0;
  std::unordered_map<unsigned, GlobalHashRing> global_hash_ring_map;

  // std::unordered_map<unsigned, LocalHashRing> local_hash_ring_map;
  // std::unordered_map<Key, KeyInfo> placement;
  // std::unordered_map<Key, unsigned> key_size_map;

  ServerThread wt;
  zmq::context_t context;
  SocketCache pushers = SocketCache(&context, ZMQ_PUSH);
  Serializer* serializer;
  MemoryKVS* kvs;
  RoutingHandlerTest() {
    kvs = new MemoryKVS();
    serializer = new MemorySerializer(kvs);
    wt = ServerThread(ip, thread_id);
    global_hash_ring_map[1].insert(ip, thread_id);
  }
  virtual ~RoutingHandlerTest() {
    delete kvs;
    delete serializer;
  }
};

TEST_F(RoutingHandlerTest, Seed) {
  EXPECT_EQ(global_hash_ring_map[1].size(), 3000);

  auto start_time = std::chrono::system_clock::now();
  auto start_time_ms =
      std::chrono::time_point_cast<std::chrono::milliseconds>(start_time);
  auto value = start_time_ms.time_since_epoch();
  unsigned long long duration = value.count();

  std::string serialized = seed_handler(logger, global_hash_ring_map, duration);

  EXPECT_EQ(global_hash_ring_map[1].size(), 3000);

  TierMembership membership;
  membership.ParseFromString(serialized);

  // check serialized duration
  unsigned long long resp_duration = membership.start_time();
  EXPECT_EQ(resp_duration, duration);

  // check serialized tier size, tier_id, ip
  EXPECT_EQ(membership.tiers_size(), 1);
  for (const auto& tier : membership.tiers()) {
    for (const std::string& other_ip : tier.ips()) {
      EXPECT_EQ(tier.tier_id(), 1);
      EXPECT_EQ(other_ip, ip);
    }
  }
}

// TEST_F(RoutingHandlerTest, NodeDepart) {
//   global_hash_ring_map[1].insert("1.0.0.1", 0);

//   EXPECT_EQ(global_hash_ring_map[1].size(), 6000);
//   std::string serialized = "1:1.0.0.1";
//   node_depart_handler(thread_id, ip, global_hash_ring_map, logger,
//   serialized,
//                       pushers);

//   EXPECT_EQ(global_hash_ring_map[1].size(), 3000);
// }

// TEST_F(RoutingHandlerTest, SelfDepart) {
//   unsigned seed = 0;
//   std::vector<Address> routing_address;
//   std::vector<Address> monitoring_address;

//   EXPECT_EQ(global_hash_ring_map[1].size(), 3000);
//   std::string serialized = "tcp://1.0.0.1:6560";
//   self_depart_handler(thread_id, seed, ip, logger, serialized,
//                       global_hash_ring_map, local_hash_ring_map,
//                       key_size_map, placement, routing_address,
//                       monitoring_address, wt, pushers, serializer);

//   EXPECT_EQ(global_hash_ring_map[1].size(), 0);
// }
