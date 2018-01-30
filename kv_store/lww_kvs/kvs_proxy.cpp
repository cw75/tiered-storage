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
#include "tbb/concurrent_unordered_map.h"
#include "rc_kv_store.h"
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"
#include "consistent_hash_map.hpp"
#include "common.h"
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>

using namespace std;

void get_global_replication_factor_proxy(
    string key,
    global_hash_t& global_memory_hash_ring,
    unordered_map<string, key_info>& placement,
    SocketCache& requesters,
    unsigned& seed) {
  string target_address;
  auto threads = responsible_global(key + "_replication", METADATA_MEMORY_REPLICATION_FACTOR, global_memory_hash_ring);
  target_address = next(begin(threads), rand_r(&seed) % threads.size())->get_request_handling_connect_addr();
  communication::Request req;
  req.set_type("GET");
  prepare_get_tuple(req, key + "_replication");
  auto response = send_request<communication::Request, communication::Response>(req, requesters[target_address]);
  if (response.tuple(0).err_number() == 0) {
    vector<string> rep_factors;
    split(response.tuple(0).value(), ':', rep_factors);
    placement[key] = key_info(stoi(rep_factors[0]), stoi(rep_factors[1]));
  } else {
    // TODO: ADD RETRY (hash ring inconsistency issue)
    placement[key] = key_info(DEFAULT_GLOBAL_MEMORY_REPLICATION, DEFAULT_GLOBAL_EBS_REPLICATION);
  }
}

void get_local_replication_factor_proxy(
    string key,
    string ip,
    global_hash_t& global_memory_hash_ring,
    unordered_map<string, key_info>& placement,
    SocketCache& requesters,
    unsigned& seed) {
  string target_address;
  auto threads = responsible_global(key + "_" + ip + "_replication", METADATA_MEMORY_REPLICATION_FACTOR, global_memory_hash_ring);
  target_address = next(begin(threads), rand_r(&seed) % threads.size())->get_request_handling_connect_addr();
  communication::Request req;
  req.set_type("GET");
  prepare_get_tuple(req, key + "_" + ip + "_replication");
  auto response = send_request<communication::Request, communication::Response>(req, requesters[target_address]);
  if (response.tuple(0).err_number() == 0) {
    placement[key].local_replication_[ip] = stoi(response.tuple(0).value());
  } else {
    // TODO: ADD RETRY (hash ring inconsistency issue)
    placement[key].local_replication_[ip] = DEFAULT_LOCAL_REPLICATION;
  }
}

// get all threads responsible for a key from the "node_type" tier
// metadata flag = 0 means the key is a metadata. Otherwise, it is a regular data
unordered_set<server_thread_t, thread_hash> get_responsible_threads_proxy(
    string key,
    unsigned metadata_flag,
    global_hash_t& global_memory_hash_ring,
    global_hash_t& global_ebs_hash_ring,
    local_hash_t& local_memory_hash_ring,
    local_hash_t& local_ebs_hash_ring,
    unordered_map<string, key_info>& placement,
    SocketCache& requesters,
    unordered_set<string>& node_types,
    unsigned& seed) {
  if (metadata_flag == 0) {
    return responsible_global(key, METADATA_MEMORY_REPLICATION_FACTOR, global_memory_hash_ring);
  } else {
    if (placement.find(key) == placement.end()) {
      get_global_replication_factor_proxy(key, global_memory_hash_ring, placement, requesters, seed);
    }

    unordered_set<server_thread_t, thread_hash> result;
    unsigned rep;
    global_hash_t* global_hash_ring;
    local_hash_t* local_hash_ring;

    for (auto tp_it = node_types.begin(); tp_it != node_types.end(); tp_it++) {
      if (*tp_it == "M") {
        rep = placement[key].global_memory_replication_;
        global_hash_ring = &global_memory_hash_ring;
        local_hash_ring = &local_memory_hash_ring;
      } else {
        rep = placement[key].global_ebs_replication_;
        global_hash_ring = &global_ebs_hash_ring;
        local_hash_ring = &local_ebs_hash_ring;
      }

      auto mts = responsible_global(key, rep, *global_hash_ring);
      for (auto it = mts.begin(); it != mts.end(); it++) {
        string ip = it->get_ip();
        if (placement[key].local_replication_.find(ip) == placement[key].local_replication_.end()) {
          get_local_replication_factor_proxy(key, ip, global_memory_hash_ring, placement, requesters, seed);
        }
        auto tids = responsible_local(key, placement[key].local_replication_[ip], *local_hash_ring);
        for (auto iter = tids.begin(); iter != tids.end(); iter++) {
          result.insert(server_thread_t(ip, *iter, *tp_it));
        }
      }
    }
    return result;
  }
}

void run(unsigned thread_id) {

  string log_file = "log_" + to_string(thread_id) + ".txt";
  string logger_name = "basic_logger_" + to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  string ip = get_ip("proxy");

  proxy_thread_t pt = proxy_thread_t(ip, thread_id);

  unsigned seed = thread_id;

  // prepare the zmq context
  zmq::context_t context(1);

  SocketCache pushers(&context, ZMQ_PUSH);
  SocketCache requesters(&context, ZMQ_REQ);

  unordered_map<string, key_info> placement;

  if (thread_id == 0) {
    string ip_line;
    ifstream address;
    vector<string> monitoring_address;

    // read existing monitoring nodes
    address.open("conf/proxy/monitoring_address.txt");

    while (getline(address, ip_line)) {
      cerr << ip_line << "\n";
      monitoring_address.push_back(ip_line);
    }
    address.close();

    // notify monitoring nodes
    for (auto it = monitoring_address.begin(); it != monitoring_address.end(); it++) {
      zmq_util::send_string("join:P:" + ip, &pushers[monitoring_thread_t(*it).get_notify_connect_addr()]);
    }
  }

  global_hash_t global_memory_hash_ring;
  global_hash_t global_ebs_hash_ring;
  local_hash_t local_memory_hash_ring;
  local_hash_t local_ebs_hash_ring;

  // form the local hash ring for both tiers
  for (unsigned tid = 1; tid <= MEMORY_THREAD_NUM; tid++) {
    local_memory_hash_ring.insert(tid);
  }
  for (unsigned tid = 1; tid <= EBS_THREAD_NUM; tid++) {
    local_ebs_hash_ring.insert(tid);
  }

  // responsible for both node join and departure
  zmq::socket_t notify_puller(context, ZMQ_PULL);
  notify_puller.bind(pt.get_notify_bind_addr());
  // responsible for handling key replication factor change requests from server nodes
  zmq::socket_t replication_factor_puller(context, ZMQ_PULL);
  replication_factor_puller.bind(pt.get_replication_factor_bind_addr());
  // responsible for handling key replication factor change requests from server nodes
  zmq::socket_t key_address_responder(context, ZMQ_REP);
  key_address_responder.bind(pt.get_key_address_bind_addr());  

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(notify_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(replication_factor_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(key_address_responder), 0, ZMQ_POLLIN, 0 }
  };

  while (true) {
    zmq_util::poll(-1, &pollitems);

    // handle a join or depart event coming from the server side
    if (pollitems[0].revents & ZMQ_POLLIN) {
      string message = zmq_util::recv_string(&notify_puller);
      if (thread_id == 0) {
        // tell all worker threads about the message
        for (unsigned tid = 1; tid < PROXY_THREAD_NUM; tid++) {
          zmq_util::send_string(message, &pushers[proxy_thread_t(ip, tid).get_notify_connect_addr()]);
        }
      }

      vector<string> v;
      split(message, ':', v);
      if (v[0] == "join") {
        cerr << "received join\n";
        // update hash ring
        if (v[1] == "M") {
          global_memory_hash_ring.insert(server_thread_t(v[2], 0, "M"));
        } else if (v[1] == "E") {
          global_ebs_hash_ring.insert(server_thread_t(v[2], 0, "E"));
        } else {
          cerr << "Invalid Tier info\n";
        }
        cerr << "memory hash ring size is " + to_string(global_memory_hash_ring.size()) + "\n";
        cerr << "ebs hash ring size is " + to_string(global_ebs_hash_ring.size()) + "\n";
      } else if (v[0] == "depart") {
        cerr << "received depart\n";
        // update hash ring
        if (v[1] == "M") {
          global_memory_hash_ring.erase(server_thread_t(v[2], 0, "M"));
        } else if (v[1] == "E") {
          global_ebs_hash_ring.erase(server_thread_t(v[2], 0, "E"));
        } else {
          cerr << "Invalid Tier info\n";
        }
        cerr << "memory hash ring size is " + to_string(global_memory_hash_ring.size()) + "\n";
        cerr << "ebs hash ring size is " + to_string(global_ebs_hash_ring.size()) + "\n";
      }
    }

    if (pollitems[1].revents & ZMQ_POLLIN) {
      cerr << "received replication factor change\n";
      string serialized_req = zmq_util::recv_string(&replication_factor_puller);

      if (thread_id == 0) {
        // tell all worker threads about the replication factor change
        for (unsigned tid = 1; tid < PROXY_THREAD_NUM; tid++) {
          zmq_util::send_string(serialized_req, &pushers[proxy_thread_t(ip, tid).get_replication_factor_connect_addr()]);
        }
      }

      communication::Replication_Factor_Request req;
      req.ParseFromString(serialized_req);

      for (int i = 0; i < req.tuple_size(); i++) {
        string key = req.tuple(i).key();
        // update the replication factor
        placement[key].global_memory_replication_ = req.tuple(i).global_memory_replication();
        placement[key].global_ebs_replication_ = req.tuple(i).global_ebs_replication();
        for (int j = 0; j < req.tuple(i).local_size(); j++) {
          placement[key].local_replication_[req.tuple(i).local(j).ip()] = req.tuple(i).local(j).local_replication();
        }
      }
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      //cerr << "received key address request\n";
      string serialized_key_req = zmq_util::recv_string(&key_address_responder);
      communication::Key_Request key_req;
      key_req.ParseFromString(serialized_key_req);

      string source_tier = key_req.source_tier();
      unsigned metadata = key_req.metadata();
      string address_type = key_req.address_type();
      unordered_set<string> node_types;
      if (source_tier == "M") {
        node_types.insert("E");
      } else if (source_tier == "E") {
        node_types.insert("M");
      } else if (source_tier == "U") {
        node_types.insert("M");
        node_types.insert("E");
      }

      communication::Key_Response key_res;

      for (int i = 0; i < key_req.tuple_size(); i++) {
        communication::Key_Response_Tuple* tp = key_res.add_tuple();
        string key = key_req.tuple(i).key();
        tp->set_key(key);
        auto threads = get_responsible_threads_proxy(key, metadata, global_memory_hash_ring, global_ebs_hash_ring, local_memory_hash_ring, local_ebs_hash_ring, placement, requesters, node_types, seed);
        if (address_type == "RH") {
          for (auto it = threads.begin(); it != threads.end(); it++) {
            communication::Key_Response_Address* ad = tp->add_address();
            ad->set_addr(it->get_request_handling_connect_addr());
          }
        } else if (address_type == "RP") {
          for (auto it = threads.begin(); it != threads.end(); it++) {
            communication::Key_Response_Address* ad = tp->add_address();
            ad->set_addr(it->get_request_pulling_connect_addr());
          }
        } else if (address_type == "G") {
          for (auto it = threads.begin(); it != threads.end(); it++) {
            communication::Key_Response_Address* ad = tp->add_address();
            ad->set_addr(it->get_gossip_connect_addr());
          }
        } else {
          cerr << "Invalid address type\n";
        }
      }
      // send the key address response
      string serialized_key_res;
      key_res.SerializeToString(&serialized_key_res);
      zmq_util::send_string(serialized_key_res, &key_address_responder);
    }
  }
}

int main(int argc, char* argv[]) {
  if (argc != 1) {
    cerr << "usage:" << argv[0] << endl;
    return 1;
  }

  vector<thread> proxy_worker_threads;

  for (unsigned thread_id = 1; thread_id < PROXY_THREAD_NUM; thread_id++) {
    proxy_worker_threads.push_back(thread(run, thread_id));
  }

  run(0);
}