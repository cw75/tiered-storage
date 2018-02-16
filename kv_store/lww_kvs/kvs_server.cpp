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
#include "rc_kv_store.h"
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"
#include "consistent_hash_map.hpp"
#include "common.h"
#include "server_utility.h"

// TODO: Everything that's currently writing to cout and cerr should be replaced with a logfile.
using namespace std;

unsigned SELF_TIER_ID;

// worker thread number
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
    chrono::system_clock::time_point& start_time,
    unordered_map<string, pair<chrono::system_clock::time_point, vector<pending_request>>>& pending_request_map,
    unsigned& seed) {
  communication::Response response;
  vector<unsigned> tier_ids;
  tier_ids.push_back(SELF_TIER_ID);
  bool succeed;
  if (req.type() == "GET") {
    //cout << "received get by thread " << thread_id << "\n";
    for (int i = 0; i < req.tuple_size(); i++) {
      string key = req.tuple(i).key();
      //cerr << "received get by thread " + to_string(wt.get_tid()) + " on key " + req.tuple(i).key() + "\n";
      // first check if the thread is responsible for the key
      auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);
      if (succeed) {
        communication::Response_Tuple* tp = response.add_tuple();
        tp->set_key(key);
        if (threads.find(wt) == threads.end()) {
          //cerr << "wrong address by thread " + to_string(wt.get_tid()) + " on key " + req.tuple(i).key() + "\n";
          tp->set_err_number(2);
          for (auto it = threads.begin(); it != threads.end(); it++) {
            tp->add_addresses(it->get_request_pulling_connect_addr());
          }
        } else {
          //cerr << "correct address by thread " + to_string(wt.get_tid()) + " on key " + req.tuple(i).key() + "\n";
          auto res = process_get(key, serializer);
          tp->set_value(res.first.reveal().value);
          tp->set_err_number(res.second);
          //cerr << "error number is " + to_string(res.second) + "\n";
          key_stat_map[key].access_ += 1;
        }
      } else {
        string val = "";
        if (pending_request_map.find(key) == pending_request_map.end()) {
          pending_request_map[key].first = chrono::system_clock::now();
        }
        pending_request_map[key].second.push_back(pending_request("G", val, req.respond_address()));
      }
    }
  } else if (req.type() == "PUT") {
    //cout << "received put by thread " << thread_id << "\n";
    for (int i = 0; i < req.tuple_size(); i++) {
      string key = req.tuple(i).key();
      //cerr << "received put by thread " + to_string(wt.get_tid()) + " on key " + req.tuple(i).key() + "\n";
      // first check if the thread is responsible for the key
      auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);
      if (succeed) {
        communication::Response_Tuple* tp = response.add_tuple();
        tp->set_key(key);
        if (threads.find(wt) == threads.end()) {
          //cerr << "wrong address by thread " + to_string(wt.get_tid()) + " on key " + req.tuple(i).key() + "\n";
          tp->set_err_number(2);
          for (auto it = threads.begin(); it != threads.end(); it++) {
            tp->add_addresses(it->get_request_pulling_connect_addr());
          }
        } else {
          //cerr << "correct address by thread " + to_string(wt.get_tid()) + " on key " + req.tuple(i).key() + "\n";
          auto current_time = chrono::system_clock::now();
          auto ts = generate_timestamp(chrono::duration_cast<chrono::milliseconds>(current_time-start_time).count(), wt.get_tid());
          process_put(key, ts, req.tuple(i).value(), serializer, key_stat_map);
          tp->set_err_number(0);
          key_stat_map[key].access_ += 1;
          local_changeset.insert(key);
        }
      } else {
        if (pending_request_map.find(key) == pending_request_map.end()) {
          pending_request_map[key].first = chrono::system_clock::now();
        }
        if (req.has_respond_address()) {
          pending_request_map[key].second.push_back(pending_request("P", req.tuple(i).value(), req.respond_address()));
        } else {
          pending_request_map[key].second.push_back(pending_request("P", req.tuple(i).value(), ""));
        }
      }
    }
  }
  return response;
}

void process_gossip(communication::Request& gossip,
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
  for (int i = 0; i < gossip.tuple_size(); i++) {
    // first check if the thread is responsible for the key
    string key = gossip.tuple(i).key();
    auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);
    if (succeed) {
      if (threads.find(wt) != threads.end()) {
        process_put(gossip.tuple(i).key(), gossip.tuple(i).timestamp(), gossip.tuple(i).value(), serializer, key_stat_map);
      }
    } else {
      if (pending_gossip_map.find(key) == pending_gossip_map.end()) {
        pending_gossip_map[key].first = chrono::system_clock::now();
      }
      pending_gossip_map[key].second.push_back(pending_gossip(gossip.tuple(i).value(), gossip.tuple(i).timestamp()));
    }
  }
}

void send_gossip(address_keyset_map& addr_keyset_map, SocketCache& pushers, Serializer* serializer) {
  unordered_map<string, communication::Request> gossip_map;

  for (auto map_it = addr_keyset_map.begin(); map_it != addr_keyset_map.end(); map_it++) {
    gossip_map[map_it->first].set_type("PUT");
    for (auto set_it = map_it->second.begin(); set_it != map_it->second.end(); set_it++) {
      auto res = process_get(*set_it, serializer);
      if (res.second == 0) {
        //cerr << "gossiping key " + *set_it + " to address " + map_it->first + "\n";
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
  string logger_name = "basic_logger_" + to_string(thread_id);
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

  // pending events for asynchrony
  unordered_map<string, pair<chrono::system_clock::time_point, vector<pending_request>>> pending_request_map;
  unordered_map<string, pair<chrono::system_clock::time_point, vector<pending_gossip>>> pending_gossip_map;

  unordered_map<string, key_info> placement;

  vector<string> proxy_address;

  vector<string> monitoring_address;

  // read address of proxies from conf file
  string ip_line;
  ifstream address;
  address.open("conf/server/proxy_address.txt");
  while (getline(address, ip_line)) {
    proxy_address.push_back(ip_line);
  }
  address.close();

  // read address of monitoring nodes from conf file
  address.open("conf/server/monitoring_address.txt");
  while (getline(address, ip_line)) {
    monitoring_address.push_back(ip_line);
  }
  address.close();

  address.open("conf/server/seed_server.txt");
  getline(address, ip_line);
  address.close();

  cerr << "seed address is " + ip_line + "\n";

  // request server addresses from the seed node
  zmq::socket_t addr_requester(context, ZMQ_REQ);
  addr_requester.connect(proxy_thread_t(ip_line, 0).get_seed_connect_addr());
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
    cerr << "Invalid node type\n";
  }

  // the set of changes made on this thread since the last round of gossip
  unordered_set<string> local_changeset;

  // keep track of the key stat
  unordered_map<string, key_stat> key_stat_map;


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
      //cerr << "thread " + to_string(thread_id) + " entering event 1\n";
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
            logger->info("hash ring for tier {} size is {}", to_string(it->first), to_string(it->second.size()));
          }
        }
        if (tier == SELF_TIER_ID) {
          vector<unsigned> tier_ids;
          tier_ids.push_back(SELF_TIER_ID);
          // map from worker address to a set of keys
          address_keyset_map addr_keyset_map;
          // keep track of which key should be removed
          unordered_set<string> remove_set;
          bool succeed;
          for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
            string key = it->first;
            auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);
            if (succeed) {
              if (threads.find(wt) == threads.end()) {
                remove_set.insert(key);
                for (auto iter = threads.begin(); iter != threads.end(); iter++) {
                  if (iter->get_ip() == new_server_ip) {
                    addr_keyset_map[iter->get_gossip_connect_addr()].insert(key);
                  }
                }
              }
            } else {
              cerr << "Error: key missing replication factor in node join routine\n";
            }
          }

          send_gossip(addr_keyset_map, pushers, serializer);
          // remove keys
          for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
            key_stat_map.erase(*it);
            serializer->remove(*it);
          }
        }
      }
      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[0] += time_elapsed;
      //cerr << "thread " + to_string(thread_id) + " leaving event 1\n";
    }

    // receives a node departure notice
    if (pollitems[1].revents & ZMQ_POLLIN) {
      //cerr << "thread " + to_string(thread_id) + " entering event 2\n";
      auto work_start = chrono::system_clock::now();
      string message = zmq_util::recv_string(&depart_puller);

      vector<string> v;
      split(message, ':', v);
      unsigned tier = stoi(v[0]);
      string departing_server_ip = v[1];
      logger->info("Received departure for node {} on tier {}", departing_server_ip, tier);
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
      //cerr << "thread " + to_string(thread_id) + " leaving event 2\n";
    }

    // receives a node departure request
    if (pollitems[2].revents & ZMQ_POLLIN) {
      //cerr << "thread " + to_string(thread_id) + " entering event 3\n";
      auto work_start = chrono::system_clock::now();
      string ack_addr = zmq_util::recv_string(&self_depart_puller);
      logger->info("Node is departing");
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
          cerr << "Error: key missing replication factor in node depart routine\n";
        }
      }

      send_gossip(addr_keyset_map, pushers, serializer);
      zmq_util::send_string(ip + "_" + to_string(SELF_TIER_ID), &pushers[ack_addr]);
      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[2] += time_elapsed;
      //cerr << "thread " + to_string(thread_id) + " leaving event 3\n";
    }

    // receives a request
    if (pollitems[3].revents & ZMQ_POLLIN) {
      //cerr << "thread " + to_string(thread_id) + " entering event 4\n";
      auto work_start = chrono::system_clock::now();
      string serialized_req = zmq_util::recv_string(&request_puller);
      communication::Request req;
      req.ParseFromString(serialized_req);
      //  process request
      auto response = process_request(req, local_changeset, serializer, wt, global_hash_ring_map, local_hash_ring_map, placement, pushers, key_stat_map, start_time, pending_request_map, seed);
      if (response.tuple_size() > 0 && req.has_respond_address()) {
        string serialized_response;
        response.SerializeToString(&serialized_response);
        //  send response
        zmq_util::send_string(serialized_response, &pushers[req.respond_address()]);
      }
      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[3] += time_elapsed;
      //cerr << "thread " + to_string(thread_id) + " leaving event 4\n";
    }

    // receives a gossip
    if (pollitems[4].revents & ZMQ_POLLIN) {
      //cerr << "thread " + to_string(thread_id) + " entering event 6\n";
      auto work_start = chrono::system_clock::now();
      string serialized_gossip = zmq_util::recv_string(&gossip_puller);
      communication::Request gossip;
      gossip.ParseFromString(serialized_gossip);
      //  Process distributed gossip
      process_gossip(gossip, wt, global_hash_ring_map, local_hash_ring_map, placement, pushers, serializer, key_stat_map, pending_gossip_map, seed);
      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[4] += time_elapsed;
      //cerr << "thread " + to_string(thread_id) + " leaving event 6\n";
    }

    // receives replication factor response
    if (pollitems[5].revents & ZMQ_POLLIN) {
      //cerr << "thread " + to_string(thread_id) + " entering event 6\n";
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
          placement[key].local_replication_map_[rep_data.local(i).ip()] = rep_data.local(i).local_replication();
        }
      } else if (response.tuple(0).err_number() == 2) {
        logger->info("Retrying rep factor query for key {}", key);
        auto respond_address = wt.get_replication_factor_connect_addr();
        issue_replication_factor_request(respond_address, key, global_hash_ring_map[1], local_hash_ring_map[1], pushers, seed);
      } else {
        for (unsigned i = MIN_TIER; i <= MAX_TIER; i++) {
          placement[key].global_replication_map_[i] = tier_data_map[i].default_replication_;
        }
      }

      if (response.tuple(0).err_number() != 2) {
        // process pending events
        vector<unsigned> tier_ids;
        tier_ids.push_back(SELF_TIER_ID);
        bool succeed;
        // pending requests
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
                communication::Response_Tuple* tp = response.add_tuple();
                tp->set_key(key);
                tp->set_err_number(2);
                for (auto iter = threads.begin(); iter != threads.end(); iter++) {
                  tp->add_addresses(iter->get_request_pulling_connect_addr());
                }
                string serialized_response;
                response.SerializeToString(&serialized_response);
                //  send response
                zmq_util::send_string(serialized_response, &pushers[it->addr_]);
              } else if (responsible && it->addr_ == "") {
                // only put requests should fall into this category
                if (it->type_ == "P") {
                  auto current_time = chrono::system_clock::now();
                  auto ts = generate_timestamp(chrono::duration_cast<chrono::milliseconds>(current_time-start_time).count(), wt.get_tid());
                  process_put(key, ts, it->value_, serializer, key_stat_map);
                  key_stat_map[key].access_ += 1;
                  local_changeset.insert(key);
                } else {
                  cerr << "Error: GET request with no respond address\n";
                }
              } else if (responsible && it->addr_ != "") {
                communication::Response response;
                communication::Response_Tuple* tp = response.add_tuple();
                tp->set_key(key);
                if (it->type_ == "G") {
                  auto res = process_get(key, serializer);
                  tp->set_value(res.first.reveal().value);
                  tp->set_err_number(res.second);
                  key_stat_map[key].access_ += 1;
                } else {
                  auto current_time = chrono::system_clock::now();
                  auto ts = generate_timestamp(chrono::duration_cast<chrono::milliseconds>(current_time-start_time).count(), wt.get_tid());
                  process_put(key, ts, it->value_, serializer, key_stat_map);
                  tp->set_err_number(0);
                  key_stat_map[key].access_ += 1;
                  local_changeset.insert(key);
                }
                string serialized_response;
                response.SerializeToString(&serialized_response);
                //  send response
                zmq_util::send_string(serialized_response, &pushers[it->addr_]);
              }
            }
          } else {
            cerr << "Error: key missing replication factor in process pending request routine\n";
          }
          pending_request_map.erase(key);
        }
        // pending gossip
        if (pending_gossip_map.find(key) != pending_gossip_map.end()) {
          auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);
          if (succeed && threads.find(wt) != threads.end()) {
            for (auto it = pending_gossip_map[key].second.begin(); it != pending_gossip_map[key].second.end(); it++) {
              process_put(key, it->ts_, it->value_, serializer, key_stat_map);
            }
          } else if (!succeed) {
            cerr << "Error: key missing replication factor in process pending gossip routine\n";
          }
          pending_gossip_map.erase(key);
        }
      }
      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[5] += time_elapsed;
      //cerr << "thread " + to_string(thread_id) + " leaving event 6\n";
    }

    // receives replication factor change
    if (pollitems[6].revents & ZMQ_POLLIN) {
      //cerr << "thread " + to_string(thread_id) + " entering event 7\n";
      auto work_start = chrono::system_clock::now();
      logger->info("Received replication factor change");
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
      // keep track of which key should be removed
      unordered_set<string> remove_set;

      // for every key, update the replication factor and 
      // check if the node is still responsible for the key
      vector<unsigned> tier_ids;
      for (unsigned i = MIN_TIER; i <= MAX_TIER; i++) {
        tier_ids.push_back(i);
      }
      bool succeed;
      for (int i = 0; i < req.tuple_size(); i++) {
        string key = req.tuple(i).key();
        // update the replication factor
        for (int j = 0; j < req.tuple(i).global_size(); j++) {
          placement[key].global_replication_map_[req.tuple(i).global(j).tier_id()] = req.tuple(i).global(j).global_replication();
        }
        for (int j = 0; j < req.tuple(i).local_size(); j++) {
          placement[key].local_replication_map_[req.tuple(i).local(j).ip()] = req.tuple(i).local(j).local_replication();
        }

        // proceed only if it is originally responsible for the key
        if (key_stat_map.find(key) != key_stat_map.end()) {
          auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);
          if (succeed) {
            if (threads.find(wt) == threads.end()) {
              remove_set.insert(key);
            }
            for (auto it = threads.begin(); it != threads.end(); it++) {
              if (it->get_id() != wt.get_id()) {
                addr_keyset_map[it->get_gossip_connect_addr()].insert(key);
              }
            }
          } else {
            cerr << "Error: key missing replication factor in rep factor change routine\n";
          }
        }
      }

      send_gossip(addr_keyset_map, pushers, serializer);

      // remove keys
      for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
        key_stat_map.erase(*it);
        serializer->remove(*it);
      }
      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[6] += time_elapsed;
      //cerr << "thread " + to_string(thread_id) + " leaving event 7\n";
    }

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
            cerr << "Error: key missing replication factor in gossip send routine\n";
          }
        }

        send_gossip(addr_keyset_map, pushers, serializer);
        local_changeset.clear();
      }
      gossip_start = chrono::system_clock::now();
      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[7] += time_elapsed;
      //cerr << "thread " + to_string(thread_id) + " leaving event gossip\n";
    }

    report_end = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(report_end-report_start).count();
    if (duration >= SERVER_REPORT_THRESHOLD) {
      //cerr << "thread " + to_string(thread_id) + " entering event report\n";
      // report server stats
      epoch += 1;
      string key = wt.get_ip() + "_" + to_string(wt.get_tid()) + "_" + to_string(SELF_TIER_ID) + "_stat";
      // compute total storage consumption
      unsigned long long consumption = 0;
      for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
        consumption += it->second.size_;
      }
      // log time
      for (auto it = working_time_map.begin(); it != working_time_map.end(); it++) {
        double event_occupancy = (double) it->second / (double) duration;
        if (event_occupancy > 0.03) {
          logger->info("event {} occupancy is {}", to_string(it->first), to_string(event_occupancy));
        }
      }
      // compute occupancy
      double occupancy = (double) working_time / (double) duration;
      if (occupancy > 0.03) {
        logger->info("occupancy is {}", to_string(occupancy));
      }
      communication::Server_Stat stat;
      stat.set_storage_consumption(consumption);
      stat.set_occupancy(occupancy);
      stat.set_epoch(epoch);
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

      /*if (epoch % 50 == 1) {
        for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
          cerr << "thread " + to_string(thread_id) + " epoch " + to_string(epoch) + " key " + it->first + " has length " + to_string(it->second.size_) + "\n";
        }
      }*/

      // report key access stats
      key = wt.get_ip() + "_" + to_string(wt.get_tid()) + "_" + to_string(SELF_TIER_ID) + "_access";
      // prepare key access stat
      communication::Key_Access access;
      for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
        communication::Key_Access_Tuple* tp = access.add_tuple();
        tp->set_key(it->first);
        tp->set_access(it->second.access_);
      }
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
      //cerr << "thread " + to_string(thread_id) + " leaving event report\n";
    }

    // check pending events and garbage collect
    unordered_set<string> remove_set;
    for (auto it = pending_request_map.begin(); it != pending_request_map.end(); it++) {
      auto t = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now()-it->second.first).count();
      if (t > GARBAGE_COLLECTION_THRESHOLD) {
        logger->info("Request GC {}", it->first);
        remove_set.insert(it->first);
      }
    }
    for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
      pending_request_map.erase(*it);
    }
    remove_set.clear();
    for (auto it = pending_gossip_map.begin(); it != pending_gossip_map.end(); it++) {
      auto t = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now()-it->second.first).count();
      if (t > GARBAGE_COLLECTION_THRESHOLD) {
        logger->info("Gossip GC {}", it->first);
        remove_set.insert(it->first);
      }
    }
    for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
      pending_gossip_map.erase(*it);
    }
  }
}

int main(int argc, char* argv[]) {

  if (argc != 1) {
    cerr << "usage:" << argv[0] << endl;
    return 1;
  }

  // populate metadata
  SELF_TIER_ID = atoi(getenv("SERVER_TYPE"));

  // debugging
  cerr << "tier id is " + to_string(SELF_TIER_ID) + "\n";

  tier_data_map[1] = tier_data(MEMORY_THREAD_NUM, DEFAULT_GLOBAL_MEMORY_REPLICATION);
  tier_data_map[2] = tier_data(EBS_THREAD_NUM, DEFAULT_GLOBAL_EBS_REPLICATION);

  THREAD_NUM = tier_data_map[SELF_TIER_ID].thread_number_;

  // debugging
  cerr << "worker thread number is " + to_string(THREAD_NUM) + "\n";

  vector<thread> worker_threads;

  // start the initial threads based on THREAD_NUM
  for (unsigned thread_id = 1; thread_id < THREAD_NUM; thread_id++) {
    worker_threads.push_back(thread(run, thread_id));
  }

  run(0);
}