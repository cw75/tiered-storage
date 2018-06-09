#include <zmq.hpp>
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <cstdio>
#include <pthread.h>
#include <unistd.h>
#include <memory>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <ctime>
#include "kvs/rc_kv_store.h"
#include "message.pb.h"
#include "zmq/socket_cache.h"
#include "zmq/zmq_util.h"
#include "utils/consistent_hash_map.hpp"
#include "common.h"
#include "utils/server_utility.h"
#include "yaml-cpp/yaml.h"
#include "yaml-cpp/node/node.h"

// TODO: Everything that's currently writing to cout and cerr should be replaced with a logfile.
using namespace std;

unsigned SELF_TIER_ID;

// number of worker threads
unsigned THREAD_NUM;

// read-only per-tier metadata
unordered_map<unsigned, tier_data> tier_data_map;

pair<RC_KVS_PairLattice<string>, unsigned> process_get(const string& key, Serializer* serializer) {
  unsigned err_number = 0;
  auto res = serializer->get(key, err_number);

  // check if the value is an empty string
  if (res.reveal().value == "") {
    err_number = 1;
  }
  return pair<RC_KVS_PairLattice<string>, unsigned>(res, err_number);
}

void process_put(const string& key,
    const unsigned long long& timestamp,
    const string& value,
    Serializer* serializer,
    unordered_map<string, key_stat>& key_stat_map) {
  
  if (serializer->put(key, value, timestamp)) {
    // update value size if the value is replaced
    key_stat_map[key].size_ = value.size();
  }
}

communication::Response process_request(
    communication::Request& req,
    unordered_set<string>& local_changeset,
    Serializer* serializer,
    server_thread_t& wt,
    unordered_map<unsigned, global_hash_t>& global_hash_ring_map,
    unordered_map<unsigned, local_hash_t>& local_hash_ring_map,
    unordered_map<string, key_info>& placement,
    SocketCache& pushers,
    unordered_map<string, key_stat>& key_stat_map,
    unordered_map<string, multiset<std::chrono::time_point<std::chrono::system_clock>>>& key_access_timestamp,
    unsigned& total_access,
    chrono::system_clock::time_point& start_time,
    unordered_map<string, pair<chrono::system_clock::time_point, vector<pending_request>>>& pending_request_map,
    unsigned& seed) {

  communication::Response response;
  string respond_id = "";
  
  if (req.has_request_id()) {
    respond_id = req.request_id();
    response.set_response_id(respond_id);
  }

  vector<unsigned> tier_ids;
  tier_ids.push_back(SELF_TIER_ID);
  bool succeed;

  if (req.type() == "GET") {
    for (int i = 0; i < req.tuple_size(); i++) {
      // first check if the thread is responsible for the key
      string key = req.tuple(i).key();
      auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

      if (succeed) {
        if (threads.find(wt) == threads.end()) {
          if (is_metadata(key)) {
            communication::Response_Tuple* tp = response.add_tuple();

            tp->set_key(key);
            tp->set_err_number(2);
          } else {
            //placement.erase(key);
            issue_replication_factor_request(wt.get_replication_factor_connect_addr(), key, global_hash_ring_map[1], local_hash_ring_map[1], pushers, seed);
            string val = "";

            if (pending_request_map.find(key) == pending_request_map.end()) {
              pending_request_map[key].first = chrono::system_clock::now();
            }

            pending_request_map[key].second.push_back(pending_request("G", val, req.respond_address(), respond_id));
          }
        } else {
          communication::Response_Tuple* tp = response.add_tuple();
          tp->set_key(key);

          auto res = process_get(key, serializer);
          tp->set_value(res.first.reveal().value);
          tp->set_err_number(res.second);

          if (req.tuple(i).has_num_address() && req.tuple(i).num_address() != threads.size()) {
            tp->set_invalidate(true);
          }

          key_access_timestamp[key].insert(std::chrono::system_clock::now());
          total_access += 1;
        }
      } else {
        string val = "";

        if (pending_request_map.find(key) == pending_request_map.end()) {
          pending_request_map[key].first = chrono::system_clock::now();
        }

        pending_request_map[key].second.push_back(pending_request("G", val, req.respond_address(), respond_id));
      }
    }
  } else if (req.type() == "PUT") {
    for (int i = 0; i < req.tuple_size(); i++) {
      // first check if the thread is responsible for the key
      string key = req.tuple(i).key();
      auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

      if (succeed) {
        if (threads.find(wt) == threads.end()) {
          if (is_metadata(key)) {
            communication::Response_Tuple* tp = response.add_tuple();

            tp->set_key(key);
            tp->set_err_number(2);
          } else {
            issue_replication_factor_request(wt.get_replication_factor_connect_addr(), key, global_hash_ring_map[1], local_hash_ring_map[1], pushers, seed);

            if (pending_request_map.find(key) == pending_request_map.end()) {
              pending_request_map[key].first = chrono::system_clock::now();
            }

            if (req.has_respond_address()) {
              pending_request_map[key].second.push_back(pending_request("P", req.tuple(i).value(), req.respond_address(), respond_id));
            } else {
              pending_request_map[key].second.push_back(pending_request("P", req.tuple(i).value(), "", respond_id));
            }
          }
        } else {
          communication::Response_Tuple* tp = response.add_tuple();
          tp->set_key(key);

          auto current_time = chrono::system_clock::now();
          auto ts = generate_timestamp(chrono::duration_cast<chrono::milliseconds>(current_time-start_time).count(), wt.get_tid());
          
          process_put(key, ts, req.tuple(i).value(), serializer, key_stat_map);
          tp->set_err_number(0);
          
          if (req.tuple(i).has_num_address() && req.tuple(i).num_address() != threads.size()) {
            tp->set_invalidate(true);
          }

          key_access_timestamp[key].insert(std::chrono::system_clock::now());
          total_access += 1;
          local_changeset.insert(key);
        }
      } else {
        if (pending_request_map.find(key) == pending_request_map.end()) {
          pending_request_map[key].first = chrono::system_clock::now();
        }

        if (req.has_respond_address()) {
          pending_request_map[key].second.push_back(pending_request("P", req.tuple(i).value(), req.respond_address(), respond_id));
        } else {
          pending_request_map[key].second.push_back(pending_request("P", req.tuple(i).value(), "", respond_id));
        }
      }
    }
  }
  return response;
}

void process_gossip(
    communication::Request& gossip,
    server_thread_t& wt,
    unordered_map<unsigned, global_hash_t>& global_hash_ring_map,
    unordered_map<unsigned, local_hash_t>& local_hash_ring_map,
    unordered_map<string, key_info>& placement,
    SocketCache& pushers,
    Serializer* serializer,
    unordered_map<string, key_stat>& key_stat_map,
    unordered_map<string, pair<chrono::system_clock::time_point, vector<pending_gossip>>>& pending_gossip_map,
    unsigned& seed) {
  vector<unsigned> tier_ids;
  tier_ids.push_back(SELF_TIER_ID);
  bool succeed;
  unordered_map<string, communication::Request> gossip_map;

  for (int i = 0; i < gossip.tuple_size(); i++) {
    // first check if the thread is responsible for the key
    string key = gossip.tuple(i).key();
    auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

    if (succeed) {
      if (threads.find(wt) != threads.end()) {
        process_put(gossip.tuple(i).key(), gossip.tuple(i).timestamp(), gossip.tuple(i).value(), serializer, key_stat_map);
      } else {
        if (is_metadata(key)) {
        // forward the gossip
          for (auto it = threads.begin(); it != threads.end(); it++) {
            if (gossip_map.find(it->get_gossip_connect_addr()) == gossip_map.end()) {
              gossip_map[it->get_gossip_connect_addr()].set_type("PUT");
            }

            prepare_put_tuple(gossip_map[it->get_gossip_connect_addr()], key, gossip.tuple(i).value(), gossip.tuple(i).timestamp());
          }
        } else {
          issue_replication_factor_request(wt.get_replication_factor_connect_addr(), key, global_hash_ring_map[1], local_hash_ring_map[1], pushers, seed);

          if (pending_gossip_map.find(key) == pending_gossip_map.end()) {
            pending_gossip_map[key].first = chrono::system_clock::now();
          }

          pending_gossip_map[key].second.push_back(pending_gossip(gossip.tuple(i).value(), gossip.tuple(i).timestamp()));
        }
      }
    } else {
      if (pending_gossip_map.find(key) == pending_gossip_map.end()) {
        pending_gossip_map[key].first = chrono::system_clock::now();
      }

      pending_gossip_map[key].second.push_back(pending_gossip(gossip.tuple(i).value(), gossip.tuple(i).timestamp()));
    }
  }

  // redirect gossip
  for (auto it = gossip_map.begin(); it != gossip_map.end(); it++) {
    push_request(it->second, pushers[it->first]);
  }
}

void send_gossip(address_keyset_map& addr_keyset_map, SocketCache& pushers, Serializer* serializer) {
  unordered_map<string, communication::Request> gossip_map;

  for (auto map_it = addr_keyset_map.begin(); map_it != addr_keyset_map.end(); map_it++) {
    gossip_map[map_it->first].set_type("PUT");

    for (auto set_it = map_it->second.begin(); set_it != map_it->second.end(); set_it++) {
      auto res = process_get(*set_it, serializer);
      if (res.second == 0) {
        prepare_put_tuple(gossip_map[map_it->first], *set_it, res.first.reveal().value, res.first.reveal().timestamp);
      }
    }
  }

  // send gossip
  for (auto it = gossip_map.begin(); it != gossip_map.end(); it++) {
    push_request(it->second, pushers[it->first]);
  }
}

// thread entry point
void run(unsigned thread_id) {
  string log_file = "log_" + to_string(thread_id) + ".txt";
  string logger_name = "server_logger_" + to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  string ip = get_ip("server");

  // each thread has a handle to itself
  server_thread_t wt = server_thread_t(ip, thread_id);

  unsigned seed = time(NULL);
  seed += thread_id;

  // prepare the zmq context
  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  // initialize hash ring maps
  unordered_map<unsigned, global_hash_t> global_hash_ring_map;
  unordered_map<unsigned, local_hash_t> local_hash_ring_map;

  // for periodically redistributing data when node joins
  address_keyset_map join_addr_keyset_map;
  
  // keep track of which key should be removed when node joins
  unordered_set<string> join_remove_set;

  // pending events for asynchrony
  unordered_map<string, pair<chrono::system_clock::time_point, vector<pending_request>>> pending_request_map;
  unordered_map<string, pair<chrono::system_clock::time_point, vector<pending_gossip>>> pending_gossip_map;
  
  unordered_map<string, key_info> placement;
  vector<string> proxy_address;
  vector<string> monitoring_address;

  // read the YAML conf
  // TODO: change this to read multiple monitoring IPs
  YAML::Node conf = YAML::LoadFile("conf/config.yml");
  string seed_ip = conf["seed_ip"].as<string>();
  monitoring_address.push_back(conf["monitoring_ip"].as<string>());
  YAML::Node proxy = conf["proxy_ip"];

  for (YAML::const_iterator it = proxy.begin(); it != proxy.end(); ++it) {
    proxy_address.push_back(it->as<string>());
  }

  // request server addresses from the seed node
  zmq::socket_t addr_requester(context, ZMQ_REQ);
  addr_requester.connect(proxy_thread_t(seed_ip, 0).get_seed_connect_addr());
  zmq_util::send_string("join", &addr_requester);

  // receive and add all the addresses that seed node sent
  string serialized_addresses = zmq_util::recv_string(&addr_requester);
  communication::Address addresses;
  addresses.ParseFromString(serialized_addresses);

  // populate start time
  unsigned long long duration = addresses.start_time();
  chrono::milliseconds dur(duration);
  chrono::system_clock::time_point start_time(dur);

  // populate addresses
  for (int i = 0; i < addresses.tuple_size(); i++) {
    insert_to_hash_ring<global_hash_t>(global_hash_ring_map[addresses.tuple(i).tier_id()], addresses.tuple(i).ip(), 0);
  }

  // add itself to global hash ring
  insert_to_hash_ring<global_hash_t>(global_hash_ring_map[SELF_TIER_ID], ip, 0);

  // form local hash rings
  for (auto it = tier_data_map.begin(); it != tier_data_map.end(); it++) {
    for (unsigned tid = 0; tid < it->second.thread_number_; tid++) {
      insert_to_hash_ring<local_hash_t>(local_hash_ring_map[it->first], ip, tid);
    }
  }

  // thread 0 notifies other servers that it has joined
  if (thread_id == 0) {
    for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
      unsigned tier_id = it->first;
      auto hash_ring = &(it->second);
      unordered_set<string> observed_ip;

      for (auto iter = hash_ring->begin(); iter != hash_ring->end(); iter++) {
        if (iter->second.get_ip().compare(ip) != 0 && observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
          zmq_util::send_string(to_string(SELF_TIER_ID) + ":" + ip, &pushers[(iter->second).get_node_join_connect_addr()]);
          observed_ip.insert(iter->second.get_ip());
        }
      }
    }

    string msg = "join:" + to_string(SELF_TIER_ID) + ":" + ip;

    // notify proxies that this node has joined
    for (auto it = proxy_address.begin(); it != proxy_address.end(); it++) {
      zmq_util::send_string(msg, &pushers[proxy_thread_t(*it, 0).get_notify_connect_addr()]);
    }

    // notify monitoring nodes that this node has joined
    for (auto it = monitoring_address.begin(); it != monitoring_address.end(); it++) {
      zmq_util::send_string(msg, &pushers[monitoring_thread_t(*it).get_notify_connect_addr()]);
    }
  }

  Serializer* serializer;

  if (SELF_TIER_ID == 1) {
    Database* kvs = new Database();
    serializer = new Memory_Serializer(kvs);
  } else if (SELF_TIER_ID == 2) {
    serializer = new EBS_Serializer(thread_id);
  } else {
    logger->info("Invalid node type");
  }

  // the set of changes made on this thread since the last round of gossip
  unordered_set<string> local_changeset;

  // keep track of the key stat
  unordered_map<string, key_stat> key_stat_map;
  // keep track of key access timestamp
  unordered_map<string, multiset<std::chrono::time_point<std::chrono::system_clock>>> key_access_timestamp;
  // keep track of total access
  unsigned total_access;

  // listens for a new node joining
  zmq::socket_t join_puller(context, ZMQ_PULL);
  join_puller.bind(wt.get_node_join_bind_addr());

  // listens for a node departing
  zmq::socket_t depart_puller(context, ZMQ_PULL);
  depart_puller.bind(wt.get_node_depart_bind_addr());

  // responsible for listening for a command that this node should leave
  zmq::socket_t self_depart_puller(context, ZMQ_PULL);
  self_depart_puller.bind(wt.get_self_depart_bind_addr());

  // responsible for handling requests
  zmq::socket_t request_puller(context, ZMQ_PULL);
  request_puller.bind(wt.get_request_pulling_bind_addr());

  // responsible for processing gossips
  zmq::socket_t gossip_puller(context, ZMQ_PULL);
  gossip_puller.bind(wt.get_gossip_bind_addr());

  // responsible for listening for key replication factor response
  zmq::socket_t replication_factor_puller(context, ZMQ_PULL);
  replication_factor_puller.bind(wt.get_replication_factor_bind_addr());

  // responsible for listening for key replication factor change
  zmq::socket_t replication_factor_change_puller(context, ZMQ_PULL);
  replication_factor_change_puller.bind(wt.get_replication_factor_change_bind_addr());

  //  Initialize poll set
  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(depart_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(self_depart_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(request_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(gossip_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(replication_factor_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(replication_factor_change_puller), 0, ZMQ_POLLIN, 0 }
  };

  auto gossip_start = chrono::system_clock::now();
  auto gossip_end = chrono::system_clock::now();
  auto report_start = chrono::system_clock::now();
  auto report_end = chrono::system_clock::now();

  unsigned long long working_time = 0;
  unordered_map<unsigned, unsigned long long> working_time_map;
  for (unsigned i = 0; i < 8; i++) {
    working_time_map[i] = 0;
  }
  unsigned epoch = 0;
  // enter event loop
  while (true) {
    zmq_util::poll(0, &pollitems);

    // receives a node join
    if (pollitems[0].revents & ZMQ_POLLIN) {
      auto work_start = chrono::system_clock::now();
      string message = zmq_util::recv_string(&join_puller);

      vector<string> v;
      split(message, ':', v);
      unsigned tier = stoi(v[0]);
      string new_server_ip = v[1];

      // update global hash ring
      bool inserted = insert_to_hash_ring<global_hash_t>(global_hash_ring_map[tier], new_server_ip, 0);

      if (inserted) {
        logger->info("Received a node join for tier {}. New node is {}", tier, new_server_ip);

        // only relevant to thread 0
        if (thread_id == 0) {
          // gossip the new node address between server nodes to ensure consistency
          for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
            unsigned tier_id = it->first;
            auto hash_ring = &(it->second);
            unordered_set<string> observed_ip;
            for (auto iter = hash_ring->begin(); iter != hash_ring->end(); iter++) {
              if (iter->second.get_ip().compare(ip) != 0 && iter->second.get_ip().compare(new_server_ip) != 0 && observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
                // if the node is not myself and not the newly joined node, send the ip of the newly joined node
                zmq_util::send_string(message, &pushers[(iter->second).get_node_join_connect_addr()]);
                observed_ip.insert(iter->second.get_ip());
              } else if (iter->second.get_ip().compare(new_server_ip) == 0 && observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
                // if the node is the newly joined node, send my ip
                zmq_util::send_string(to_string(SELF_TIER_ID) + ":" + ip, &pushers[(iter->second).get_node_join_connect_addr()]);
                observed_ip.insert(iter->second.get_ip());
              }
            }
          }

          // tell all worker threads about the new node join
          for (unsigned tid = 1; tid < THREAD_NUM; tid++) {
            zmq_util::send_string(message, &pushers[server_thread_t(ip, tid).get_node_join_connect_addr()]);
          }

          for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
            logger->info("Hash ring for tier {} is size {}.", to_string(it->first), to_string(it->second.size()));
          }
        }

        if (tier == SELF_TIER_ID) {
          vector<unsigned> tier_ids;
          tier_ids.push_back(SELF_TIER_ID);
          bool succeed;

          for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
            string key = it->first;
            auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

            if (succeed) {
              if (threads.find(wt) == threads.end()) {
                join_remove_set.insert(key);

                for (auto iter = threads.begin(); iter != threads.end(); iter++) {
                  join_addr_keyset_map[iter->get_gossip_connect_addr()].insert(key);
                }
              }
            } else {
              logger->info("Error: missing key replication factor in node join routine.");
            }
          }
        }
      }

      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[0] += time_elapsed;
    }

    // receive a node departure notice
    if (pollitems[1].revents & ZMQ_POLLIN) {
      auto work_start = chrono::system_clock::now();
      string message = zmq_util::recv_string(&depart_puller);

      vector<string> v;
      split(message, ':', v);

      unsigned tier = stoi(v[0]);
      string departing_server_ip = v[1];
      logger->info("Received departure for node {} on tier {}.", departing_server_ip, tier);

      // update hash ring
      remove_from_hash_ring<global_hash_t>(global_hash_ring_map[tier], departing_server_ip, 0);

      if (thread_id == 0) {
        // tell all worker threads about the node departure
        for (unsigned tid = 1; tid < THREAD_NUM; tid++) {
          zmq_util::send_string(message, &pushers[server_thread_t(ip, tid).get_node_depart_connect_addr()]);
        }

        for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
          logger->info("hash ring for tier {} size is {}", to_string(it->first), to_string(it->second.size()));
        }
      }

      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[1] += time_elapsed;
    }

    // receives a node departure request
    if (pollitems[2].revents & ZMQ_POLLIN) {
      auto work_start = chrono::system_clock::now();
      string ack_addr = zmq_util::recv_string(&self_depart_puller);

      logger->info("Node is departing.");
      remove_from_hash_ring<global_hash_t>(global_hash_ring_map[SELF_TIER_ID], ip, 0);

      if (thread_id == 0) {
        for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
          auto hash_ring = &(it->second);
          unordered_set<string> observed_ip;

          for (auto iter = hash_ring->begin(); iter != hash_ring->end(); iter++) {
            if (observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
              zmq_util::send_string(to_string(SELF_TIER_ID) + ":" + ip, &pushers[(iter->second).get_node_depart_connect_addr()]);
              observed_ip.insert(iter->second.get_ip());
            }
          }
        }

        string msg = "depart:" + to_string(SELF_TIER_ID) + ":" + ip;

        // notify proxies
        for (auto it = proxy_address.begin(); it != proxy_address.end(); it++) {
          zmq_util::send_string(msg, &pushers[proxy_thread_t(*it, 0).get_notify_connect_addr()]);
        }

        // notify monitoring nodes
        for (auto it = monitoring_address.begin(); it != monitoring_address.end(); it++) {
          zmq_util::send_string(msg, &pushers[monitoring_thread_t(*it).get_notify_connect_addr()]);
        }

        // tell all worker threads about the self departure
        for (unsigned tid = 1; tid < THREAD_NUM; tid++) {
          zmq_util::send_string(ack_addr, &pushers[server_thread_t(ip, tid).get_self_depart_connect_addr()]);
        }
      }

      address_keyset_map addr_keyset_map;
      vector<unsigned> tier_ids;

      for (unsigned i = MIN_TIER; i <= MAX_TIER; i++) {
        tier_ids.push_back(i);
      }

      bool succeed;

      for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
        string key = it->first;
        auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

        if (succeed) {
          // since we already removed itself from the hash ring, no need to exclude itself from threads
          for (auto iter = threads.begin(); iter != threads.end(); iter++) {
            addr_keyset_map[iter->get_gossip_connect_addr()].insert(key);
          }
        } else {
          logger->info("Error: key missing replication factor in node depart routine");
        }
      }

      send_gossip(addr_keyset_map, pushers, serializer);
      zmq_util::send_string(ip + "_" + to_string(SELF_TIER_ID), &pushers[ack_addr]);

      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[2] += time_elapsed;
    }

    // receive a request
    if (pollitems[3].revents & ZMQ_POLLIN) {
      auto work_start = chrono::system_clock::now();
      string serialized_req = zmq_util::recv_string(&request_puller);
      communication::Request req;
      req.ParseFromString(serialized_req);

      // process request
      auto response = process_request(req, local_changeset, serializer, wt, global_hash_ring_map, local_hash_ring_map, placement, pushers, key_stat_map, key_access_timestamp, total_access, start_time, pending_request_map, seed);

      if (response.tuple_size() > 0 && req.has_respond_address()) {
        string serialized_response;
        response.SerializeToString(&serialized_response);
        zmq_util::send_string(serialized_response, &pushers[req.respond_address()]);
      }

      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[3] += time_elapsed;
    }

    // receive gossip
    if (pollitems[4].revents & ZMQ_POLLIN) {
      auto work_start = chrono::system_clock::now();
      string serialized_gossip = zmq_util::recv_string(&gossip_puller);
      communication::Request gossip;
      gossip.ParseFromString(serialized_gossip);

      //  Process distributed gossip
      process_gossip(gossip, wt, global_hash_ring_map, local_hash_ring_map, placement, pushers, serializer, key_stat_map, pending_gossip_map, seed);
      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();

      working_time += time_elapsed;
      working_time_map[4] += time_elapsed;
    }

    // receives replication factor response
    if (pollitems[5].revents & ZMQ_POLLIN) {
      auto work_start = chrono::system_clock::now();
      string serialized_response = zmq_util::recv_string(&replication_factor_puller);
      communication::Response response;
      response.ParseFromString(serialized_response);

      vector<string> tokens;
      split(response.tuple(0).key(), '_', tokens);
      string key = tokens[0];

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
        auto respond_address = wt.get_replication_factor_connect_addr();
        issue_replication_factor_request(respond_address, key, global_hash_ring_map[1], local_hash_ring_map[1], pushers, seed);
      } else {
        for (unsigned i = MIN_TIER; i <= MAX_TIER; i++) {
          placement[key].global_replication_map_[i] = tier_data_map[i].default_replication_;
          placement[key].local_replication_map_[i] = DEFAULT_LOCAL_REPLICATION;
        }
      }

      if (response.tuple(0).err_number() != 2) {
        // process pending events
        vector<unsigned> tier_ids;
        tier_ids.push_back(SELF_TIER_ID);
        bool succeed;

        // process pending requests
        if (pending_request_map.find(key) != pending_request_map.end()) {
          auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

          if (succeed) {
            bool responsible;

            if (threads.find(wt) != threads.end()) {
              responsible = true;
            } else {
              responsible = false;
            }

            for (auto it = pending_request_map[key].second.begin(); it != pending_request_map[key].second.end(); it++) {
              if (!responsible && it->addr_ != "") {
                communication::Response response;
                
                if (it->respond_id_ != "") {
                  response.set_response_id(it->respond_id_);
                }

                communication::Response_Tuple* tp = response.add_tuple();
                tp->set_key(key);
                tp->set_err_number(2);

                for (auto iter = threads.begin(); iter != threads.end(); iter++) {
                  tp->add_addresses(iter->get_request_pulling_connect_addr());
                }

                string serialized_response;
                response.SerializeToString(&serialized_response);
                zmq_util::send_string(serialized_response, &pushers[it->addr_]);
              } else if (responsible && it->addr_ == "") {
                // only put requests should fall into this category
                if (it->type_ == "P") {
                  auto current_time = chrono::system_clock::now();
                  auto ts = generate_timestamp(chrono::duration_cast<chrono::milliseconds>(current_time-start_time).count(), wt.get_tid());

                  process_put(key, ts, it->value_, serializer, key_stat_map);
                  key_access_timestamp[key].insert(std::chrono::system_clock::now());

                  total_access += 1;
                  local_changeset.insert(key);
                } else {
                  logger->info("Error: GET request with no response address.");
                }
              } else if (responsible && it->addr_ != "") {
                communication::Response response;

                if (it->respond_id_ != "") {
                  response.set_response_id(it->respond_id_);
                }

                communication::Response_Tuple* tp = response.add_tuple();
                tp->set_key(key);

                if (it->type_ == "G") {
                  auto res = process_get(key, serializer);
                  tp->set_value(res.first.reveal().value);
                  tp->set_err_number(res.second);

                  key_access_timestamp[key].insert(std::chrono::system_clock::now());
                  total_access += 1;
                } else {
                  auto current_time = chrono::system_clock::now();
                  auto ts = generate_timestamp(chrono::duration_cast<chrono::milliseconds>(current_time-start_time).count(), wt.get_tid());

                  process_put(key, ts, it->value_, serializer, key_stat_map);
                  tp->set_err_number(0);

                  key_access_timestamp[key].insert(std::chrono::system_clock::now());
                  total_access += 1;
                  local_changeset.insert(key);
                }

                string serialized_response;
                response.SerializeToString(&serialized_response);
                zmq_util::send_string(serialized_response, &pushers[it->addr_]);
              }
            }
          } else {
            logger->info("Error: missing key replication factor in process pending request routine.");
          }
          pending_request_map.erase(key);
        }

        // process pending gossip
        if (pending_gossip_map.find(key) != pending_gossip_map.end()) {
          auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

          if (succeed) {
            if (threads.find(wt) != threads.end()) {
              for (auto it = pending_gossip_map[key].second.begin(); it != pending_gossip_map[key].second.end(); it++) {
                process_put(key, it->ts_, it->value_, serializer, key_stat_map);
              }
            } else {
              unordered_map<string, communication::Request> gossip_map;
              
              // forward the gossip
              for (auto it = threads.begin(); it != threads.end(); it++) {
                gossip_map[it->get_gossip_connect_addr()].set_type("PUT");

                for (auto iter = pending_gossip_map[key].second.begin(); iter != pending_gossip_map[key].second.end(); iter++) {
                  prepare_put_tuple(gossip_map[it->get_gossip_connect_addr()], key, iter->value_, iter->ts_);
                }
              }

              // redirect gossip
              for (auto it = gossip_map.begin(); it != gossip_map.end(); it++) {
                push_request(it->second, pushers[it->first]);
              }
            }
          } else {
            logger->info("Error: missing key replication factor in process pending gossip routine.");
          }
          pending_gossip_map.erase(key);
        }
      }

      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[5] += time_elapsed;
    }

    // receive replication factor change
    if (pollitems[6].revents & ZMQ_POLLIN) {
      auto work_start = chrono::system_clock::now();
      logger->info("Received replication factor change.");
      string serialized_req = zmq_util::recv_string(&replication_factor_change_puller);

      if (thread_id == 0) {
        // tell all worker threads about the replication factor change
        for (unsigned tid = 1; tid < THREAD_NUM; tid++) {
          zmq_util::send_string(serialized_req, &pushers[server_thread_t(ip, tid).get_replication_factor_change_connect_addr()]);
        }
      }

      communication::Replication_Factor_Request req;
      req.ParseFromString(serialized_req);

      address_keyset_map addr_keyset_map;
      unordered_set<string> remove_set;

      // for every key, update the replication factor and check if the node is still responsible for the key
      vector<unsigned> tier_ids;
      for (unsigned i = MIN_TIER; i <= MAX_TIER; i++) {
        tier_ids.push_back(i);
      }

      bool succeed;

      for (int i = 0; i < req.tuple_size(); i++) {
        string key = req.tuple(i).key();

        if (key_stat_map.find(key) != key_stat_map.end()) {
          auto orig_threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

          if (succeed) {
            // update the replication factor
            bool decrement = false;
            for (int j = 0; j < req.tuple(i).global_size(); j++) {
              if (req.tuple(i).global(j).global_replication() < placement[key].global_replication_map_[req.tuple(i).global(j).tier_id()]) {
                decrement = true;
              }

              placement[key].global_replication_map_[req.tuple(i).global(j).tier_id()] = req.tuple(i).global(j).global_replication();
            }

            for (int j = 0; j < req.tuple(i).local_size(); j++) {
              if (req.tuple(i).local(j).local_replication() < placement[key].local_replication_map_[req.tuple(i).local(j).tier_id()]) {
                decrement = true;
              }

              placement[key].local_replication_map_[req.tuple(i).local(j).tier_id()] = req.tuple(i).local(j).local_replication();
            }

            auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

            if (succeed) {
              if (threads.find(wt) == threads.end()) {
                remove_set.insert(key);

                for (auto it = threads.begin(); it != threads.end(); it++) {
                  addr_keyset_map[it->get_gossip_connect_addr()].insert(key);
                }
              }

              if (!decrement && orig_threads.begin()->get_id() == wt.get_id()) {
                unordered_set<server_thread_t, thread_hash> new_threads;

                for (auto it = threads.begin(); it != threads.end(); it++) {
                  if (orig_threads.find(*it) == orig_threads.end()) {
                    new_threads.insert(*it);
                  }
                }

                for (auto it = new_threads.begin(); it != new_threads.end(); it++) {
                  addr_keyset_map[it->get_gossip_connect_addr()].insert(key);
                }
              }
            } else {
              logger->info("Error: missing key replication factor in rep factor change routine.");
            }
          } else {
            logger->info("Error: missing key replication factor in rep factor change routine.");

            // just update the replication factor
            for (int j = 0; j < req.tuple(i).global_size(); j++) {
              placement[key].global_replication_map_[req.tuple(i).global(j).tier_id()] = req.tuple(i).global(j).global_replication();
            }

            for (int j = 0; j < req.tuple(i).local_size(); j++) {
              placement[key].local_replication_map_[req.tuple(i).local(j).tier_id()] = req.tuple(i).local(j).local_replication();
            }
          }
        } else {
          // just update the replication factor
          for (int j = 0; j < req.tuple(i).global_size(); j++) {
            placement[key].global_replication_map_[req.tuple(i).global(j).tier_id()] = req.tuple(i).global(j).global_replication();
          }

          for (int j = 0; j < req.tuple(i).local_size(); j++) {
            placement[key].local_replication_map_[req.tuple(i).local(j).tier_id()] = req.tuple(i).local(j).local_replication();
          }
        }
      }

      send_gossip(addr_keyset_map, pushers, serializer);

      // remove keys
      for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
        key_stat_map.erase(*it);
        serializer->remove(*it);
        local_changeset.erase(*it);
      }

      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[6] += time_elapsed;
    }

    // gossip updates to other threads
    gossip_end = chrono::system_clock::now();
    if (chrono::duration_cast<chrono::microseconds>(gossip_end-gossip_start).count() >= PERIOD) {
      //cerr << "thread " + to_string(thread_id) + " entering event gossip\n";
      auto work_start = chrono::system_clock::now();
      // only gossip if we have changes
      if (local_changeset.size() > 0) {
        address_keyset_map addr_keyset_map;

        vector<unsigned> tier_ids;
        for (unsigned i = MIN_TIER; i <= MAX_TIER; i++) {
          tier_ids.push_back(i);
        }

        bool succeed;
        for (auto it = local_changeset.begin(); it != local_changeset.end(); it++) {
          string key = *it;
          auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

          if (succeed) {
            for (auto iter = threads.begin(); iter != threads.end(); iter++) {
              if (iter->get_id() != wt.get_id()) {
                addr_keyset_map[iter->get_gossip_connect_addr()].insert(key);
              }
            }
          } else {
            logger->info("Error: missing key replication factor in gossip send routine.");
          }
        }

        send_gossip(addr_keyset_map, pushers, serializer);
        local_changeset.clear();
      }

      gossip_start = chrono::system_clock::now();
      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();

      working_time += time_elapsed;
      working_time_map[7] += time_elapsed;
    }

    // collect and store internal statistics
    report_end = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::seconds>(report_end-report_start).count();

    if (duration >= SERVER_REPORT_THRESHOLD) {
      epoch += 1;
      string key = wt.get_ip() + "_" + to_string(wt.get_tid()) + "_" + to_string(SELF_TIER_ID) + "_stat";

      // compute total storage consumption
      unsigned long long consumption = 0;
      for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
        consumption += it->second.size_;
      }

      for (auto it = working_time_map.begin(); it != working_time_map.end(); it++) {
        // cast to microsecond
        double event_occupancy = (double) it->second / ((double) duration * 1000000);

        if (event_occupancy > 0.02) {
          logger->info("Event {} occupancy is {}.", to_string(it->first), to_string(event_occupancy));
        }
      }

      double occupancy = (double) working_time / ((double) duration * 1000000);
      if (occupancy > 0.02) {
        logger->info("Occupancy is {}.", to_string(occupancy));
      }

      communication::Server_Stat stat;
      stat.set_storage_consumption(consumption/1000); // cast to KB
      stat.set_occupancy(occupancy);
      stat.set_epoch(epoch);
      stat.set_total_access(total_access);

      string serialized_stat;
      stat.SerializeToString(&serialized_stat);

      communication::Request req;
      req.set_type("PUT");
      prepare_put_tuple(req, key, serialized_stat, 0);

      auto threads = get_responsible_threads_metadata(key, global_hash_ring_map[1], local_hash_ring_map[1]);
      if (threads.size() != 0) {
        string target_address = next(begin(threads), rand_r(&seed) % threads.size())->get_request_pulling_connect_addr();
        push_request(req, pushers[target_address]);
      }

      // compute key access stats
      communication::Key_Access access;
      auto current_time = chrono::system_clock::now();

      for (auto it = key_access_timestamp.begin(); it != key_access_timestamp.end(); it++) {
        string key = it->first;
        auto mset = &(it->second);

        // garbage collect
        for (auto set_iter = mset->rbegin(); set_iter != mset->rend(); set_iter++) {
          if (chrono::duration_cast<std::chrono::seconds>(current_time-*set_iter).count() >= KEY_MONITORING_THRESHOLD) {
            mset->erase(mset->begin(), set_iter.base());
            break;
          }
        }

        // update key_access_frequency
        communication::Key_Access_Tuple* tp = access.add_tuple();
        tp->set_key(key);
        tp->set_access(mset->size());
      }

      // report key access stats
      key = wt.get_ip() + "_" + to_string(wt.get_tid()) + "_" + to_string(SELF_TIER_ID) + "_access";
      string serialized_access;
      access.SerializeToString(&serialized_access);

      req.Clear();
      req.set_type("PUT");
      prepare_put_tuple(req, key, serialized_access, 0);

      threads = get_responsible_threads_metadata(key, global_hash_ring_map[1], local_hash_ring_map[1]);

      if (threads.size() != 0) {
        string target_address = next(begin(threads), rand_r(&seed) % threads.size())->get_request_pulling_connect_addr();
        push_request(req, pushers[target_address]);
      }

      report_start = chrono::system_clock::now();

      // reset
      working_time = 0;
      for (unsigned i = 0; i < 8; i++) {
        working_time_map[i] = 0;
      }
      // reset total access
      total_access = 0;
    }

    // retry pending gossips
    for (auto it = pending_gossip_map.begin(); it != pending_gossip_map.end(); it++) {
      auto t = chrono::duration_cast<chrono::seconds>(chrono::system_clock::now()-it->second.first).count();

      if (t > RETRY_THRESHOLD) {
        auto respond_address = wt.get_replication_factor_connect_addr();
        issue_replication_factor_request(respond_address, it->first, global_hash_ring_map[1], local_hash_ring_map[1], pushers, seed);

        // refresh time
        it->second.first = chrono::system_clock::now();
      }
    }

    //redistribute data after node joins
    if (join_addr_keyset_map.size() != 0) {
      unordered_set<string> remove_address_set;

      // assemble gossip
      address_keyset_map addr_keyset_map;
      for (auto it = join_addr_keyset_map.begin(); it != join_addr_keyset_map.end(); it++) {
        auto address = it->first;
        auto key_set = &(it->second);
        unsigned count = 0;

        while (count < DATA_REDISTRIBUTE_THRESHOLD && key_set->size() > 0) {
          string k = *(key_set->begin());
          addr_keyset_map[address].insert(k);

          key_set->erase(k);
          count += 1;
        }

        if (key_set->size() == 0) {
          remove_address_set.insert(address);
        }
      }

      for (auto it = remove_address_set.begin(); it != remove_address_set.end(); it++) {
        join_addr_keyset_map.erase(*it);
      }

      send_gossip(addr_keyset_map, pushers, serializer);

      // remove keys
      if (join_addr_keyset_map.size() == 0) {
        for (auto it = join_remove_set.begin(); it != join_remove_set.end(); it++) {
          key_stat_map.erase(*it);
          serializer->remove(*it);
        }
      }
    }
  }
}

int main(int argc, char* argv[]) {

  if (argc != 1) {
    cerr << "Usage: " << argv[0] << endl;
    return 1;
  }

  // populate metadata
  SELF_TIER_ID = atoi(getenv("SERVER_TYPE"));

  tier_data_map[1] = tier_data(MEMORY_THREAD_NUM, DEFAULT_GLOBAL_MEMORY_REPLICATION, MEM_NODE_CAPACITY);
  tier_data_map[2] = tier_data(EBS_THREAD_NUM, DEFAULT_GLOBAL_EBS_REPLICATION, EBS_NODE_CAPACITY);

  THREAD_NUM = tier_data_map[SELF_TIER_ID].thread_number_;

  // start the initial threads based on THREAD_NUM
  vector<thread> worker_threads;
  for (unsigned thread_id = 1; thread_id < THREAD_NUM; thread_id++) {
    worker_threads.push_back(thread(run, thread_id));
  }

  run(0);
}
