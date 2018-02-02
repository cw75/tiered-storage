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

// get node type
string NODE_TYPE = string(getenv("SERVER_TYPE"));

// worker thread number
unsigned THREAD_NUM;

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
    global_hash_t& global_hash_ring,
    local_hash_t& local_hash_ring,
    unordered_map<string, key_info>& placement,
    SocketCache& requesters,
    unordered_map<string, key_stat>& key_stat_map,
    vector<string>& proxy_address,
    chrono::system_clock::time_point& start_time,
    unsigned& seed) {
  communication::Response response;
  if (req.type() == "GET") {
    //cout << "received get by thread " << thread_id << "\n";
    response.set_type("GET");
    for (int i = 0; i < req.tuple_size(); i++) {
      //cerr << "received get by thread " + to_string(wt.get_tid()) + " on key " + req.tuple(i).key() + "\n";
      communication::Response_Tuple* tp = response.add_tuple();
      string key = req.tuple(i).key();
      tp->set_key(key);
      // first check if the thread is responsible for the key
      auto threads = get_responsible_threads(key, wt.get_tid(), global_hash_ring, local_hash_ring, placement, requesters, NODE_TYPE, proxy_address, seed);
      if (threads.find(wt) == threads.end()) {
        //cerr << "wrong address by thread " + to_string(wt.get_tid()) + " on key " + req.tuple(i).key() + "\n";
        tp->set_err_number(2);
        for (auto it = threads.begin(); it != threads.end(); it++) {
          communication::Response_Address* ad = tp->add_address();
          ad->set_addr(it->get_request_handling_connect_addr());
        }
      } else {
        //cerr << "correct address by thread " + to_string(wt.get_tid()) + " on key " + req.tuple(i).key() + "\n";
        auto res = process_get(key, serializer);
        tp->set_value(res.first.reveal().value);
        tp->set_timestamp(res.first.reveal().timestamp);
        tp->set_err_number(res.second);
        //cerr << "error number is " + to_string(res.second) + "\n";
        key_stat_map[key].access_ += 1;
      }
    }
  } else if (req.type() == "PUT") {
    //cout << "received put by thread " << thread_id << "\n";
    response.set_type("PUT");
    for (int i = 0; i < req.tuple_size(); i++) {
      //cerr << "received put by thread " + to_string(wt.get_tid()) + " on key " + req.tuple(i).key() + "\n";
      communication::Response_Tuple* tp = response.add_tuple();
      string key = req.tuple(i).key();
      tp->set_key(key);
      // first check if the thread is responsible for the key
      auto threads = get_responsible_threads(key, wt.get_tid(), global_hash_ring, local_hash_ring, placement, requesters, NODE_TYPE, proxy_address, seed);
      if (threads.find(wt) == threads.end()) {
        //cerr << "wrong address by thread " + to_string(wt.get_tid()) + " on key " + req.tuple(i).key() + "\n";
        tp->set_err_number(2);
        for (auto it = threads.begin(); it != threads.end(); it++) {
          communication::Response_Address* ad = tp->add_address();
          ad->set_addr(it->get_request_handling_connect_addr());
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
    }
  }
  return response;
}

void process_gossip(communication::Request& gossip,
    server_thread_t& wt,
    global_hash_t& global_hash_ring,
    local_hash_t& local_hash_ring,
    unordered_map<string, key_info>& placement,
    SocketCache& requesters,
    Serializer* serializer,
    unordered_map<string, key_stat>& key_stat_map,
    vector<string>& proxy_address,
    unsigned& seed) {
  for (int i = 0; i < gossip.tuple_size(); i++) {
    // first check if the thread is responsible for the key
    auto threads = get_responsible_threads(gossip.tuple(i).key(), wt.get_tid(), global_hash_ring, local_hash_ring, placement, requesters, NODE_TYPE, proxy_address, seed);
    if (threads.find(wt) != threads.end()) {
      process_put(gossip.tuple(i).key(), gossip.tuple(i).timestamp(), gossip.tuple(i).value(), serializer, key_stat_map);
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
void run(unsigned thread_id, string new_node) {

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
  SocketCache requesters(&context, ZMQ_REQ);

  // create our hash rings
  global_hash_t global_hash_ring;
  local_hash_t local_hash_ring;
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

  // form the global hash ring
  if (new_node == "n") {
    // add itself to the ring
    insert_to_hash_ring<global_hash_t>(global_hash_ring, ip, 0);
  } else { // get server address from the seed node
    address.open("conf/server/seed_server.txt");
    getline(address, ip_line);
    address.close();

    cerr << "seed address is " + ip_line + "\n";

    // request server addresses from the seed node
    zmq::socket_t addr_requester(context, ZMQ_REQ);
    addr_requester.connect(server_thread_t(ip_line, 0).get_seed_connect_addr());
    zmq_util::send_string("join", &addr_requester);

    // receive and add all the addresses that seed node sent
    vector<string> addresses;
    split(zmq_util::recv_string(&addr_requester), '|', addresses);
    for (auto it = addresses.begin(); it != addresses.end(); it++) {
      insert_to_hash_ring<global_hash_t>(global_hash_ring, *it, 0);
    }

    // add itself to global hash ring
    insert_to_hash_ring<global_hash_t>(global_hash_ring, ip, 0);
  }

  // form the local hash ring
  for (unsigned tid = 1; tid <= THREAD_NUM; tid++) {
    insert_to_hash_ring<local_hash_t>(local_hash_ring, ip, tid);
  }

  // the metadata thread notify other servers that it has joined
  if (thread_id == 0 && new_node == "y") {
    unordered_set<string> observed_ip;
    for (auto it = global_hash_ring.begin(); it != global_hash_ring.end(); it++) {
      if (it->second.get_ip().compare(ip) != 0 && observed_ip.find(it->second.get_ip()) == observed_ip.end()) {
        zmq_util::send_string(ip, &pushers[(it->second).get_node_join_connect_addr()]);
        observed_ip.insert(it->second.get_ip());
      }
    }
  }

  // the metadata thread notify proxies and monitoring node that it has joined
  if (thread_id == 0) {
    string msg = "join:" + NODE_TYPE + ":" + ip;
    // notify proxies that this node has joined the service
    for (auto it = proxy_address.begin(); it != proxy_address.end(); it++) {
      zmq_util::send_string(msg, &pushers[proxy_thread_t(*it, 0).get_notify_connect_addr()]);
    }

    // notify monitoring nodes that this node has joined the service
    for (auto it = monitoring_address.begin(); it != monitoring_address.end(); it++) {
      zmq_util::send_string(msg, &pushers[monitoring_thread_t(*it).get_notify_connect_addr()]);
    }
  }

  Serializer* serializer;

  if (NODE_TYPE == "M") {
    Database* kvs = new Database();
    serializer = new Memory_Serializer(kvs);
  } else if (NODE_TYPE == "E") {
    serializer = new EBS_Serializer(thread_id);
  } else {
    cerr << "Invalid node type\n";
  }

  // the set of changes made on this thread since the last round of gossip
  unordered_set<string> local_changeset;

  // keep track of the key stat
  unordered_map<string, key_stat> key_stat_map;


  // responsible for sending the server address to a new node (only relevant for metadata thread)
  zmq::socket_t addr_responder(context, ZMQ_REP);
  addr_responder.bind(wt.get_seed_bind_addr());
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
  zmq::socket_t request_responder(context, ZMQ_REP);
  request_responder.bind(wt.get_request_handling_bind_addr());
  // responsible for handling requests (no response needed)
  zmq::socket_t request_puller(context, ZMQ_PULL);
  request_puller.bind(wt.get_request_pulling_bind_addr());
  // responsible for processing gossips
  zmq::socket_t gossip_puller(context, ZMQ_PULL);
  gossip_puller.bind(wt.get_gossip_bind_addr());
  // responsible for listening for key replication factor change
  zmq::socket_t replication_factor_puller(context, ZMQ_PULL);
  replication_factor_puller.bind(wt.get_replication_factor_bind_addr());

  //  Initialize poll set
  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(addr_responder), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(depart_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(self_depart_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(request_responder), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(request_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(gossip_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(replication_factor_puller), 0, ZMQ_POLLIN, 0 }
  };

  auto start_time = chrono::system_clock::now();

  auto gossip_start = chrono::system_clock::now();
  auto gossip_end = chrono::system_clock::now();
  auto report_start = chrono::system_clock::now();
  auto report_end = chrono::system_clock::now();
  auto garbage_start = chrono::system_clock::now();
  auto garbage_end = chrono::system_clock::now();

  unsigned long long working_time = 0;
  unsigned epoch = 0;
  // enter event loop
  while (true) {
    zmq_util::poll(0, &pollitems);

    // only relavant for the metadata thread of the seed node
    if (pollitems[0].revents & ZMQ_POLLIN) {
      //cerr << "thread " + to_string(thread_id) + " entering event 0\n";
      auto work_start = chrono::system_clock::now();
      logger->info("Received an address request");
      zmq_util::recv_string(&addr_responder);

      string addresses;
      unordered_set<string> observed_ip;
      for (auto it = global_hash_ring.begin(); it != global_hash_ring.end(); it++) {
        if (observed_ip.find(it->second.get_ip()) == observed_ip.end()) {
          addresses += (it->second.get_ip() + "|");
          observed_ip.insert(it->second.get_ip());
        }
      }

      // remove the trailing pipe
      addresses.pop_back();
      zmq_util::send_string(addresses, &addr_responder);
      working_time += chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      //cerr << "thread " + to_string(thread_id) + " leaving event 0\n";
    }

    // receives a node join
    if (pollitems[1].revents & ZMQ_POLLIN) {
      //cerr << "thread " + to_string(thread_id) + " entering event 1\n";
      auto work_start = chrono::system_clock::now();
      string new_server_ip = zmq_util::recv_string(&join_puller);
      logger->info("Received a node join. New node is {}", new_server_ip);
      // update global hash ring
      bool inserted = insert_to_hash_ring<global_hash_t>(global_hash_ring, new_server_ip, 0);

      if (inserted) {
        // only relevant to the metadata thread
        if (thread_id == 0) {
          // gossip the new node address between server nodes to ensure consistency
          unordered_set<string> observed_ip;
          for (auto it = global_hash_ring.begin(); it != global_hash_ring.end(); it++) {
            if (it->second.get_ip().compare(ip) != 0 && it->second.get_ip().compare(new_server_ip) != 0 && observed_ip.find(it->second.get_ip()) == observed_ip.end()) {
              // if the node is not myself and not the newly joined node, send the ip of the newly joined node
              zmq_util::send_string(new_server_ip, &pushers[(it->second).get_node_join_connect_addr()]);
              observed_ip.insert(it->second.get_ip());
            } else if (it->second.get_ip().compare(new_server_ip) == 0 && observed_ip.find(it->second.get_ip()) == observed_ip.end()) {
              // if the node is the newly joined node, send my ip
              zmq_util::send_string(ip, &pushers[(it->second).get_node_join_connect_addr()]);
              observed_ip.insert(it->second.get_ip());
            }
          }
          // tell all worker threads about the new node join
          for (unsigned tid = 1; tid <= THREAD_NUM; tid++) {
            zmq_util::send_string(new_server_ip, &pushers[server_thread_t(ip, tid).get_node_join_connect_addr()]);
          }
        }
        // map from worker address to a set of keys
        address_keyset_map addr_keyset_map;
        // keep track of which key should be removed
        unordered_set<string> remove_set;
        for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
          string key = it->first;
          auto threads = get_responsible_threads(key, thread_id, global_hash_ring, local_hash_ring, placement, requesters, NODE_TYPE, proxy_address, seed);
          if (threads.find(wt) == threads.end()) {
            remove_set.insert(key);
            for (auto iter = threads.begin(); iter != threads.end(); iter++) {
              if (iter->get_ip() == new_server_ip) {
                addr_keyset_map[iter->get_gossip_connect_addr()].insert(key);
              }
            }
          }
        }

        send_gossip(addr_keyset_map, pushers, serializer);
        // remove keys
        for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
          key_stat_map.erase(*it);
          serializer->remove(*it);
        }
      }
      working_time += chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      //cerr << "thread " + to_string(thread_id) + " leaving event 1\n";
    }

    // receives a node departure notice
    if (pollitems[2].revents & ZMQ_POLLIN) {
      //cerr << "thread " + to_string(thread_id) + " entering event 2\n";
      auto work_start = chrono::system_clock::now();
      string departing_server_ip = zmq_util::recv_string(&depart_puller);
      logger->info("Received departure for node {}", departing_server_ip);
      // update hash ring
      remove_from_hash_ring<global_hash_t>(global_hash_ring, departing_server_ip, 0);
      if (thread_id == 0) {
        // tell all worker threads about the node departure
        for (unsigned tid = 1; tid <= THREAD_NUM; tid++) {
          zmq_util::send_string(departing_server_ip, &pushers[server_thread_t(ip, tid).get_node_depart_connect_addr()]);
        }
      }
      working_time += chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      //cerr << "thread " + to_string(thread_id) + " leaving event 2\n";
    }

    // receives a node departure request
    if (pollitems[3].revents & ZMQ_POLLIN) {
      //cerr << "thread " + to_string(thread_id) + " entering event 3\n";
      auto work_start = chrono::system_clock::now();
      string ack_addr = zmq_util::recv_string(&self_depart_puller);
      logger->info("Node is departing");
      remove_from_hash_ring<global_hash_t>(global_hash_ring, ip, 0);
      if (thread_id == 0) {
        // tell other metadata thread that the node is departing
        unordered_set<string> observed_ip;
        for (auto it = global_hash_ring.begin(); it != global_hash_ring.end(); it++) {
          if (observed_ip.find(it->second.get_ip()) == observed_ip.end()) {
            zmq_util::send_string(ip, &pushers[(it->second).get_node_depart_connect_addr()]);
            observed_ip.insert(it->second.get_ip());
          }
        }
        string msg = "depart:" + NODE_TYPE + ":" + ip;
        // notify proxies
        for (auto it = proxy_address.begin(); it != proxy_address.end(); it++) {
          zmq_util::send_string(msg, &pushers[proxy_thread_t(*it, 0).get_notify_connect_addr()]);
        }
        // notify monitoring nodes
        for (auto it = monitoring_address.begin(); it != monitoring_address.end(); it++) {
          zmq_util::send_string(msg, &pushers[monitoring_thread_t(*it).get_notify_connect_addr()]);
        }
        // tell all worker threads about the self departure
        for (unsigned tid = 1; tid <= THREAD_NUM; tid++) {
          zmq_util::send_string(ack_addr, &pushers[server_thread_t(ip, tid).get_self_depart_connect_addr()]);
        }
      }

      address_keyset_map addr_keyset_map;
      communication::Key_Request key_req;
      key_req.set_source_tier(NODE_TYPE);
      key_req.set_metadata(wt.get_tid());
      key_req.set_address_type("G");

      for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
        string key = it->first;
        auto threads = get_responsible_threads(key, thread_id, global_hash_ring, local_hash_ring, placement, requesters, NODE_TYPE, proxy_address, seed);
        // since we already removed itself from the hash ring, no need to exclude itself from threads
        for (auto iter = threads.begin(); iter != threads.end(); iter++) {
          addr_keyset_map[iter->get_gossip_connect_addr()].insert(key);
        }
        communication::Key_Request_Tuple* tp = key_req.add_tuple();
        tp->set_key(key);
      }
      // query proxy for addresses on the other tier
      string target_address = get_random_proxy_thread(proxy_address, seed).get_key_address_connect_addr();
      if (thread_id != 0) {
        query_key_address(key_req, requesters[target_address], addr_keyset_map);
      }

      send_gossip(addr_keyset_map, pushers, serializer);

      zmq_util::send_string(ip + "_" + NODE_TYPE, &pushers[ack_addr]);
      working_time += chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      //cerr << "thread " + to_string(thread_id) + " leaving event 3\n";
      break;
    }

    // receives a request
    if (pollitems[4].revents & ZMQ_POLLIN) {
      //cerr << "thread " + to_string(thread_id) + " entering event 4\n";
      auto work_start = chrono::system_clock::now();
      string serialized_req = zmq_util::recv_string(&request_responder);
      communication::Request req;
      req.ParseFromString(serialized_req);
      //  process request
      auto response = process_request(req, local_changeset, serializer, wt, global_hash_ring, local_hash_ring, placement, requesters, key_stat_map, proxy_address, start_time, seed);
      string serialized_response;
      response.SerializeToString(&serialized_response);
      //  send response
      zmq_util::send_string(serialized_response, &request_responder);
      working_time += chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      //cerr << "thread " + to_string(thread_id) + " leaving event 4\n";
    }

    // receives a request (no response needed)
    if (pollitems[5].revents & ZMQ_POLLIN) {
      //cerr << "thread " + to_string(thread_id) + " entering event 5\n";
      auto work_start = chrono::system_clock::now();
      string serialized_req = zmq_util::recv_string(&request_puller);
      communication::Request req;
      req.ParseFromString(serialized_req);
      //  process request
      process_request(req, local_changeset, serializer, wt, global_hash_ring, local_hash_ring, placement, requesters, key_stat_map, proxy_address, start_time, seed);
      working_time += chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      //cerr << "thread " + to_string(thread_id) + " leaving event 5\n";
    }

    // receives a gossip
    if (pollitems[6].revents & ZMQ_POLLIN) {
      //cerr << "thread " + to_string(thread_id) + " entering event 6\n";
      auto work_start = chrono::system_clock::now();
      string serialized_gossip = zmq_util::recv_string(&gossip_puller);
      communication::Request gossip;
      gossip.ParseFromString(serialized_gossip);
      //  Process distributed gossip
      process_gossip(gossip, wt, global_hash_ring, local_hash_ring, placement, requesters, serializer, key_stat_map, proxy_address, seed);
      working_time += chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      //cerr << "thread " + to_string(thread_id) + " leaving event 6\n";
    }

    // receives replication factor change
    if (pollitems[7].revents & ZMQ_POLLIN) {
      //cerr << "thread " + to_string(thread_id) + " entering event 7\n";
      auto work_start = chrono::system_clock::now();
      logger->info("Received replication factor change");
      string serialized_req = zmq_util::recv_string(&replication_factor_puller);

      if (thread_id == 0) {
        // tell all worker threads about the replication factor change
        for (unsigned tid = 1; tid <= THREAD_NUM; tid++) {
          zmq_util::send_string(serialized_req, &pushers[server_thread_t(ip, tid).get_replication_factor_connect_addr()]);
        }
      } else {
        // rep factor change is only relevant to worker threads
        communication::Replication_Factor_Request req;
        req.ParseFromString(serialized_req);

        address_keyset_map addr_keyset_map;
        communication::Key_Request key_req;
        key_req.set_source_tier(NODE_TYPE);
        key_req.set_metadata(wt.get_tid());
        key_req.set_address_type("G");
        // keep track of which key should be removed
        unordered_set<string> remove_set;

        // for every key, update the replication factor and 
        // check if the node is still responsible for the key
        for (int i = 0; i < req.tuple_size(); i++) {
          string key = req.tuple(i).key();
          // update the replication factor
          placement[key].global_memory_replication_ = req.tuple(i).global_memory_replication();
          placement[key].global_ebs_replication_ = req.tuple(i).global_ebs_replication();
          for (int j = 0; j < req.tuple(i).local_size(); j++) {
            placement[key].local_replication_[req.tuple(i).local(j).ip()] = req.tuple(i).local(j).local_replication();
          }

          // proceed only if it is originally responsible for the key
          if (key_stat_map.find(key) != key_stat_map.end()) {
            auto threads = get_responsible_threads(key, thread_id, global_hash_ring, local_hash_ring, placement, requesters, NODE_TYPE, proxy_address, seed);
            if (threads.find(wt) == threads.end()) {
              remove_set.insert(key);
            }
            for (auto it = threads.begin(); it != threads.end(); it++) {
              if (it->get_id() != wt.get_id()) {
                addr_keyset_map[it->get_gossip_connect_addr()].insert(key);
              }
            }
            communication::Key_Request_Tuple* tp = key_req.add_tuple();
            tp->set_key(key);
          }
        }

        // query proxy for addresses on the other tier
        string target_address = get_random_proxy_thread(proxy_address, seed).get_key_address_connect_addr();
        query_key_address(key_req, requesters[target_address], addr_keyset_map);

        send_gossip(addr_keyset_map, pushers, serializer);

        // remove keys
        for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
          key_stat_map.erase(*it);
          serializer->remove(*it);
        }
      }
      working_time += chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      //cerr << "thread " + to_string(thread_id) + " leaving event 7\n";
    }

    gossip_end = chrono::system_clock::now();
    if (chrono::duration_cast<chrono::microseconds>(gossip_end-gossip_start).count() >= PERIOD) {
      //cerr << "thread " + to_string(thread_id) + " entering event gossip\n";
      auto work_start = chrono::system_clock::now();
      // only gossip if we have changes
      if (local_changeset.size() > 0) {
        address_keyset_map addr_keyset_map;
        communication::Key_Request key_req;
        key_req.set_source_tier(NODE_TYPE);
        key_req.set_metadata(wt.get_tid());
        key_req.set_address_type("G");

        for (auto it = local_changeset.begin(); it != local_changeset.end(); it++) {
          auto threads = get_responsible_threads(*it, thread_id, global_hash_ring, local_hash_ring, placement, requesters, NODE_TYPE, proxy_address, seed);
          for (auto iter = threads.begin(); iter != threads.end(); iter++) {
            if (iter->get_id() != wt.get_id()) {
              addr_keyset_map[iter->get_gossip_connect_addr()].insert(*it);
            }
          }
          communication::Key_Request_Tuple* tp = key_req.add_tuple();
          tp->set_key(*it);
        }

        // query proxy for addresses on the other tier
        string target_address = get_random_proxy_thread(proxy_address, seed).get_key_address_connect_addr();
        if (thread_id != 0) {
          query_key_address(key_req, requesters[target_address], addr_keyset_map);
        }

        send_gossip(addr_keyset_map, pushers, serializer);
        local_changeset.clear();
      }
      gossip_start = chrono::system_clock::now();
      working_time += chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      //cerr << "thread " + to_string(thread_id) + " leaving event gossip\n";
    }

    garbage_end = chrono::system_clock::now();
    // this should be for debugging only!
    if (chrono::duration_cast<chrono::microseconds>(garbage_end-garbage_start).count() >= GARBAGE_COLLECT_THRESHOLD) {
      //cerr << "thread " + to_string(thread_id) + " entering event gc\n";
      auto work_start = chrono::system_clock::now();
      unordered_set<string> remove_set;
      // perform garbage collection
      for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
        string key = it->first;
        auto threads = get_responsible_threads(key, thread_id, global_hash_ring, local_hash_ring, placement, requesters, NODE_TYPE, proxy_address, seed);
        if (threads.find(wt) == threads.end()) {
          remove_set.insert(key);
        }
      }
      for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
        key_stat_map.erase(*it);
        serializer->remove(*it);
      }
      garbage_start = chrono::system_clock::now();
      working_time += chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      //cerr << "thread " + to_string(thread_id) + " leaving event gc\n";
    }

    report_end = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(report_end-report_start).count();
    if (duration >= SERVER_REPORT_THRESHOLD) {
      //cerr << "thread " + to_string(thread_id) + " entering event report\n";
      // report server stats
      epoch += 1;
      string key = wt.get_ip() + "_" + to_string(wt.get_tid()) + "_" + NODE_TYPE + "_stat";
      // compute total storage consumption
      unsigned long long consumption = 0;
      for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
        consumption += it->second.size_;
      }
      // compute occupancy
      double occupancy = (double) working_time / (double) duration;
      communication::Server_Stat stat;
      stat.set_storage_consumption(consumption);
      stat.set_occupancy(occupancy);
      string serialized_stat;
      stat.SerializeToString(&serialized_stat);

      communication::Request req;
      req.set_type("PUT");
      prepare_put_tuple(req, key, serialized_stat, 0);

      string target_address;

      if (NODE_TYPE == "M") {
        auto threads = get_responsible_threads(key, 0, global_hash_ring, local_hash_ring, placement, requesters, NODE_TYPE, proxy_address, seed);
        target_address = next(begin(threads), rand_r(&seed) % threads.size())->get_request_pulling_connect_addr();
      } else { // query the proxy for metadata thread address
        string target_proxy_address = get_random_proxy_thread(proxy_address, seed).get_key_address_connect_addr();
        auto addresses = get_address_from_other_tier(key, requesters[target_proxy_address], NODE_TYPE, 0, "RP");
        target_address = addresses[rand_r(&seed) % addresses.size()];
      }
      push_request(req, pushers[target_address]);

      /*if (epoch % 50 == 1) {
        for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
          cerr << "thread " + to_string(thread_id) + " epoch " + to_string(epoch) + " key " + it->first + " has length " + to_string(it->second.size_) + "\n";
        }
      }*/

      // report key access stats
      key = wt.get_ip() + "_" + to_string(wt.get_tid()) + "_" + NODE_TYPE + "_access";
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

      if (NODE_TYPE == "M") {
        auto threads = get_responsible_threads(key, 0, global_hash_ring, local_hash_ring, placement, requesters, NODE_TYPE, proxy_address, seed);
        target_address = next(begin(threads), rand_r(&seed) % threads.size())->get_request_pulling_connect_addr();
      } else { // query the proxy for metadata thread address
        string target_proxy_address = get_random_proxy_thread(proxy_address, seed).get_key_address_connect_addr();
        auto addresses = get_address_from_other_tier(key, requesters[target_proxy_address], NODE_TYPE, 0, "RP");
        target_address = addresses[rand_r(&seed) % addresses.size()];
      }
      push_request(req, pushers[target_address]);

      report_start = chrono::system_clock::now();
      working_time = 0;
      //cerr << "thread " + to_string(thread_id) + " leaving event report\n";
    }
  }
}

int main(int argc, char* argv[]) {

  if (argc != 2) {
    cerr << "usage:" << argv[0] << " <new_node>" << endl;
    return 1;
  }

  if (string(argv[1]) != "y" && string(argv[1]) != "n") {
    cerr << "Invalid first argument: " << string(argv[1]) << "." << endl;
    return 1;
  }

  // debugging
  cerr << "node type is " + NODE_TYPE + "\n";

  if (NODE_TYPE == "M") {
    THREAD_NUM = MEMORY_THREAD_NUM;
  } else if (NODE_TYPE == "E") {
    THREAD_NUM = EBS_THREAD_NUM;
  }

  // debugging
  cerr << "worker thread number is " + to_string(THREAD_NUM) + "\n";

  string new_node = argv[1];

  vector<thread> worker_threads;

  // start the initial threads based on THREAD_NUM
  for (unsigned thread_id = 1; thread_id <= THREAD_NUM; thread_id++) {
    worker_threads.push_back(thread(run, thread_id, new_node));
  }

  run(0, new_node);

  for (auto it = worker_threads.begin(); it != worker_threads.end(); it++) {
    it->join();
  }
}