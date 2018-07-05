#include <zmq.hpp>
#include <string>
#include <stdlib.h>
#include <sstream>
#include <fstream>
#include <vector>
#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include <memory>
#include "spdlog/spdlog.h"
#include "communication.pb.h"
#include "zmq/socket_cache.h"
#include "zmq/zmq_util.h"
#include "utils/consistent_hash_map.hpp"
#include "common.h"
#include "threads.h"
#include "hashers.h"
#include "hash_ring.h"
#include "yaml-cpp/yaml.h"

using namespace std;

// read-only per-tier metadata
unordered_map<unsigned, tier_data> tier_data_map;

void run(unsigned thread_id) {
  string log_file = "log_" + to_string(thread_id) + ".txt";
  string logger_name = "routing_logger_" + to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  YAML::Node conf = YAML::LoadFile("conf/config.yml")["routing"];
  string ip = conf["ip"].as<string>();

  routing_thread_t pt = routing_thread_t(ip, thread_id);

  unsigned seed = time(NULL);
  seed += thread_id;

  // prepare the zmq context
  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);
  unordered_map<string, key_info> placement;

  // warm up for benchmark
  warmup(placement);

  if (thread_id == 0) {
    // read the YAML conf
    vector<string> monitoring_address;
    YAML::Node monitoring = conf["monitoring"];

    for (YAML::const_iterator it = monitoring.begin(); it != monitoring.end(); ++it) {
      monitoring_address.push_back(it->as<string>());
    }

    // notify monitoring nodes
    for (auto it = monitoring_address.begin(); it != monitoring_address.end(); it++) {
      zmq_util::send_string("join:0:" + ip, &pushers[monitoring_thread_t(*it).get_notify_connect_addr()]);
    }
  }

  // initialize hash ring maps
  unordered_map<unsigned, global_hash_t> global_hash_ring_map;
  unordered_map<unsigned, local_hash_t> local_hash_ring_map;

  // pending events for asynchrony
  unordered_map<string, pair<chrono::system_clock::time_point, vector<pair<string, string>>>> pending_key_request_map;

  // form local hash rings
  for (auto it = tier_data_map.begin(); it != tier_data_map.end(); it++) {
    for (unsigned tid = 0; tid < it->second.thread_number_; tid++) {
      insert_to_hash_ring<local_hash_t>(local_hash_ring_map[it->first], ip, tid);
    }
  }

  // responsible for sending existing server addresses to a new node (relevant to seed node)
  zmq::socket_t addr_responder(context, ZMQ_REP);
  addr_responder.bind(pt.get_seed_bind_addr());

  // responsible for both node join and departure
  zmq::socket_t notify_puller(context, ZMQ_PULL);
  notify_puller.bind(pt.get_notify_bind_addr());

  // responsible for listening for key replication factor response
  zmq::socket_t replication_factor_puller(context, ZMQ_PULL);
  replication_factor_puller.bind(pt.get_replication_factor_bind_addr());

  // responsible for handling key replication factor change requests from server nodes
  zmq::socket_t replication_factor_change_puller(context, ZMQ_PULL);
  replication_factor_change_puller.bind(pt.get_replication_factor_change_bind_addr());

  // responsible for handling key address request from users
  zmq::socket_t key_address_puller(context, ZMQ_PULL);
  key_address_puller.bind(pt.get_key_address_bind_addr());

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(addr_responder), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(notify_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(replication_factor_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(replication_factor_change_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(key_address_puller), 0, ZMQ_POLLIN, 0 }
  };

  auto start_time = chrono::system_clock::now();
  auto start_time_ms = chrono::time_point_cast<std::chrono::milliseconds>(start_time);

  auto value = start_time_ms.time_since_epoch();
  unsigned long long duration = value.count();

  while (true) {
    zmq_util::poll(-1, &pollitems);

    // only relavant for the seed node
    if (pollitems[0].revents & ZMQ_POLLIN) {
      logger->info("Received an address request.");
      zmq_util::recv_string(&addr_responder);

      communication::Address address;
      address.set_start_time(duration);
      for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
        unsigned tier_id = it->first;
        auto hash_ring = &(it->second);
        unordered_set<string> observed_ip;

        for (auto iter = hash_ring->begin(); iter != hash_ring->end(); iter++) {
          if (observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
            communication::Address_Tuple* tp = address.add_tuple();
            tp->set_tier_id(tier_id);
            tp->set_ip(iter->second.get_ip());

            observed_ip.insert(iter->second.get_ip());
          }
        }
      }

      string serialized_address;
      address.SerializeToString(&serialized_address);
      zmq_util::send_string(serialized_address, &addr_responder);
    }

    // handle a join or depart event coming from the server side
    if (pollitems[1].revents & ZMQ_POLLIN) {
      string message = zmq_util::recv_string(&notify_puller);
      vector<string> v;

      split(message, ':', v);
      string type = v[0];
      unsigned tier = stoi(v[1]);
      string new_server_ip = v[2];

      if (type == "join") {
        logger->info("Received join from server {} in tier {}.", new_server_ip, to_string(tier));

        // update hash ring
        bool inserted = insert_to_hash_ring<global_hash_t>(global_hash_ring_map[tier], new_server_ip, 0);

        if (inserted) {
          if (thread_id == 0) {
            // gossip the new node address between server nodes to ensure consistency
            for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
              unsigned tier_id = it->first;
              auto hash_ring = &(it->second);
              unordered_set<string> observed_ip;

              for (auto iter = hash_ring->begin(); iter != hash_ring->end(); iter++) {
                // if the node is not the newly joined node, send the ip of the newly joined node
                if (iter->second.get_ip().compare(new_server_ip) != 0 && observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
                  zmq_util::send_string(to_string(tier) + ":" + new_server_ip, &pushers[(iter->second).get_node_join_connect_addr()]);
                  observed_ip.insert(iter->second.get_ip());
                }
              }
            }

            // tell all worker threads about the message
            for (unsigned tid = 1; tid < PROXY_THREAD_NUM; tid++) {
              zmq_util::send_string(message, &pushers[routing_thread_t(ip, tid).get_notify_connect_addr()]);
            }
          }
        }

        for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
          logger->info("Hash ring for tier {} size is {}.", to_string(it->first), to_string(it->second.size()));
        }
      } else if (type == "depart") {
        logger->info("Received depart from server {}.", new_server_ip);
        remove_from_hash_ring<global_hash_t>(global_hash_ring_map[tier], new_server_ip, 0);

        if (thread_id == 0) {
          // tell all worker threads about the message
          for (unsigned tid = 1; tid < PROXY_THREAD_NUM; tid++) {
            zmq_util::send_string(message, &pushers[routing_thread_t(ip, tid).get_notify_connect_addr()]);
          }
        }

        for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
          logger->info("Hash ring for tier {} size is {}.", to_string(it->first), to_string(it->second.size()));
        }
      }
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      // received replication factor response
      string serialized_response = zmq_util::recv_string(&replication_factor_puller);
      communication::Response response;
      response.ParseFromString(serialized_response);

      vector<string> tokens;
      split(response.tuple(0).key(), '_', tokens);
      string key = tokens[1];

      if (response.tuple(0).err_number() == 0) {
        communication::Replication_Factor rep_data;
        rep_data.ParseFromString(response.tuple(0).value());

        for (int i = 0; i < rep_data.global_size(); i++) {
          placement[key].global_replication_map_[rep_data.global(i).tier_id()] = rep_data.global(i).global_replication();
        }

        for (int i = 0; i < rep_data.local_size(); i++) {
          placement[key].local_replication_map_[rep_data.local(i).tier_id()] = rep_data.local(i).local_replication();
        }
      } else if (response.tuple(0).err_number() == 2) {
        logger->info("Retrying rep factor query for key {}.", key);

        auto respond_address = pt.get_replication_factor_connect_addr();
        issue_replication_factor_request(respond_address, key, global_hash_ring_map[1], local_hash_ring_map[1], pushers, seed);
      } else {
        for (unsigned i = MIN_TIER; i <= MAX_TIER; i++) {
          placement[key].global_replication_map_[i] = tier_data_map[i].default_replication_;
          placement[key].local_replication_map_[i] = DEFAULT_LOCAL_REPLICATION;
        }
      }

      if (response.tuple(0).err_number() != 2) {
        // process pending key address requests
        if (pending_key_request_map.find(key) != pending_key_request_map.end()) {
          bool succeed;
          vector<unsigned> tier_ids;

          // first check memory tier
          tier_ids.push_back(1);
          auto threads = get_responsible_threads(pt.get_replication_factor_connect_addr(), key, false, global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

          if (succeed) {
            if (threads.size() == 0) {
              // check ebs tier
              tier_ids.clear();
              tier_ids.push_back(2);
              threads = get_responsible_threads(pt.get_replication_factor_connect_addr(), key, false, global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);
            }

            for (auto it = pending_key_request_map[key].second.begin(); it != pending_key_request_map[key].second.end(); it++) {
              communication::Key_Response key_res;
              key_res.set_response_id(it->second);
              communication::Key_Response_Tuple* tp = key_res.add_tuple();
              tp->set_key(key);

              for (auto iter = threads.begin(); iter != threads.end(); iter++) {
                tp->add_addresses(iter->get_request_pulling_connect_addr());
              }

              // send the key address response
              string serialized_key_res;
              key_res.SerializeToString(&serialized_key_res);
              zmq_util::send_string(serialized_key_res, &pushers[it->first]);
            }
          } else {
            logger->info("Error: Missing replication factor for key {}.", key);
          }
          pending_key_request_map.erase(key);
        }
      }
    }

    if (pollitems[3].revents & ZMQ_POLLIN) {
      logger->info("Received a replication factor change.");
      string serialized_req = zmq_util::recv_string(&replication_factor_change_puller);

      if (thread_id == 0) {
        // tell all worker threads about the replication factor change
        for (unsigned tid = 1; tid < PROXY_THREAD_NUM; tid++) {
          zmq_util::send_string(serialized_req, &pushers[routing_thread_t(ip, tid).get_replication_factor_change_connect_addr()]);
        }
      }

      communication::Replication_Factor_Request req;
      req.ParseFromString(serialized_req);

      for (int i = 0; i < req.tuple_size(); i++) {
        string key = req.tuple(i).key();
        // update the replication factor

        for (int j = 0; j < req.tuple(i).global_size(); j++) {
          placement[key].global_replication_map_[req.tuple(i).global(j).tier_id()] = req.tuple(i).global(j).global_replication();
        }

        for (int j = 0; j < req.tuple(i).local_size(); j++) {
          placement[key].local_replication_map_[req.tuple(i).local(j).tier_id()] = req.tuple(i).local(j).local_replication();
        }
      }
    }

    if (pollitems[4].revents & ZMQ_POLLIN) {
      logger->info("Received key address request.");
      string serialized_key_req = zmq_util::recv_string(&key_address_puller);
      communication::Key_Request key_req;
      key_req.ParseFromString(serialized_key_req);

      communication::Key_Response key_res;
      key_res.set_response_id(key_req.request_id());
      bool succeed;

      for (int i = 0; i < key_req.keys_size(); i++) {
        // first check memory tier
        vector<unsigned> tier_ids;
        tier_ids.push_back(1);

        string key = key_req.keys(i);
        auto threads = get_responsible_threads(pt.get_replication_factor_connect_addr(), key, false, global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

        if (succeed) {
          if (threads.size() == 0) {
            // check ebs tier
            tier_ids.clear();
            tier_ids.push_back(2);
            threads = get_responsible_threads(pt.get_replication_factor_connect_addr(), key, false, global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);
          }

          communication::Key_Response_Tuple* tp = key_res.add_tuple();
          tp->set_key(key);

          for (auto it = threads.begin(); it != threads.end(); it++) {
            tp->add_addresses(it->get_request_pulling_connect_addr());
          }
        } else {
          if (pending_key_request_map.find(key) == pending_key_request_map.end()) {
            pending_key_request_map[key].first = chrono::system_clock::now();
          }

          pending_key_request_map[key].second.push_back(pair<string, string>(key_req.respond_address(), key_req.request_id()));
        }
      }
      if (key_res.tuple_size() > 0) {
        string serialized_key_res;
        key_res.SerializeToString(&serialized_key_res);

        zmq_util::send_string(serialized_key_res, &pushers[key_req.respond_address()]);
      }
    }
  }
}

int main(int argc, char* argv[]) {
  if (argc != 1) {
    cerr << "Usage: " << argv[0] << endl;
    return 1;
  }

  tier_data_map[1] = tier_data(MEMORY_THREAD_NUM, DEFAULT_GLOBAL_MEMORY_REPLICATION, MEM_NODE_CAPACITY);
  tier_data_map[2] = tier_data(EBS_THREAD_NUM, DEFAULT_GLOBAL_EBS_REPLICATION, EBS_NODE_CAPACITY);

  vector<thread> routing_worker_threads;

  for (unsigned thread_id = 1; thread_id < PROXY_THREAD_NUM; thread_id++) {
    routing_worker_threads.push_back(thread(run, thread_id));
  }

  run(0);
}
