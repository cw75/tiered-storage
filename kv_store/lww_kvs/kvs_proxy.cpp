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

#define MONITORING_THRESHOLD 30
#define MONITORING_PERIOD 10

using namespace std;
using address_t = string;
using address_cache_t = unordered_map<address_t, unordered_map<string, unordered_set<address_t>>>;

boost::shared_mutex memory_lock;
boost::shared_mutex ebs_lock;

struct key_access_info {
  int thread_id;
  int timestamp;
  unordered_map<string, size_t> key_access_frequency;
};

// given a key, check memory and ebs hash ring to find all the server nodes responsible for the key
vector<master_node_t> get_nodes(string key, global_hash_t* global_memory_hash_ring, global_hash_t* global_ebs_hash_ring, tbb::concurrent_unordered_map<string, shared_key_info>* placement) {

  int gmr = placement->find(key)->second.global_memory_replication_.load();
  int ger = placement->find(key)->second.global_ebs_replication_.load();
  vector<master_node_t> server_nodes;
  // use hash ring to find the right node to contact
  // first, look up the memory hash ring
  if (global_memory_hash_ring != nullptr) {
    memory_lock.lock_shared();
    auto it = global_memory_hash_ring->find(key);
    if (it != global_memory_hash_ring->end()) {
      for (int i = 0; i < gmr; i++) {
        server_nodes.push_back(it->second);
        if (++it == global_memory_hash_ring->end()) {
          it = global_memory_hash_ring->begin();
        }
      }
    }
    memory_lock.unlock_shared();
  }
  // then check the ebs hash ring
  if (global_ebs_hash_ring != nullptr) {
    ebs_lock.lock_shared();
    auto it = global_ebs_hash_ring->find(key);
    if (it != global_ebs_hash_ring->end()) {
      for (int i = 0; i < ger; i++) {
        server_nodes.push_back(it->second);
        if (++it == global_ebs_hash_ring->end()) {
          it = global_ebs_hash_ring->begin();
        }
      }
    }
    ebs_lock.unlock_shared();
  }
  return server_nodes;
}

void process_benchmark_request(string type,
    string key,
    string value,
    zmq::context_t* context,
    SocketCache& requesters,
    global_hash_t* global_memory_hash_ring,
    global_hash_t* global_ebs_hash_ring,
    monitoring_node_t& monitoring_node,
    tbb::concurrent_unordered_map<string, shared_key_info>* placement,
    address_cache_t* address_cache) {

  // if the replication factor info is missing, query the monitoring node
  if (placement->find(key) == placement->end()) {
    /*zmq_util::send_string(key, &requesters[monitoring_node.replication_factor_connect_addr_]);

    string replication_factor_response = zmq_util::recv_string(&requesters[monitoring_node.replication_factor_connect_addr_]);
    communication::Replication_Factor replication_factor;
    replication_factor.ParseFromString(replication_factor_response);*/

    placement->emplace(std::piecewise_construct,
                       std::forward_as_tuple(key),
                       std::forward_as_tuple(1, 0));
  }

  vector<master_node_t> server_nodes = get_nodes(key, global_memory_hash_ring, global_ebs_hash_ring, placement);
  // get a random node
  master_node_t server_node = server_nodes[rand() % server_nodes.size()];

  if (address_cache->find(server_node.ip_) == address_cache->end() || address_cache->at(server_node.ip_).find(key) == address_cache->at(server_node.ip_).end()) {
    communication::Key_Request key_req;
    key_req.set_sender("proxy");
    communication::Key_Request_Tuple* tp = key_req.add_tuple();
    tp->set_key(key);
    tp->set_global_memory_replication(placement->find(key)->second.global_memory_replication_.load());
    tp->set_global_ebs_replication(placement->find(key)->second.global_ebs_replication_.load());

    // serialize request and send
    string data;
    key_req.SerializeToString(&data);
    zmq_util::send_string(data, &requesters[server_node.key_exchange_connect_addr_]);

    auto send_time = std::chrono::system_clock::now();
    // wait for a response from the server and deserialize
    data = zmq_util::recv_string(&requesters[server_node.key_exchange_connect_addr_]);
    auto receive_time = std::chrono::system_clock::now();

    //cout << "key request took " + to_string(chrono::duration_cast<std::chrono::microseconds>(receive_time-send_time).count()) + " microseconds\n";

    communication::Key_Response key_res;
    key_res.ParseFromString(data);

    // update address cache
    for (int i = 0; i < key_res.tuple(0).address_size(); i++) {
      (*address_cache)[server_node.ip_][key].insert(key_res.tuple(0).address(i).addr());
    }
  }

  address_t worker_address = *(next(begin((*address_cache)[server_node.ip_][key]), rand() % (*address_cache)[server_node.ip_][key].size()));

  communication::Request req;

  if (type == "G") {
    communication::Request_Get* g = req.add_get();
    g->set_key(key);
  } else {
    communication::Request_Put* p = req.add_put();
    p->set_key(key);
    p->set_value(value);
  }

  string req_data;
  req.SerializeToString(&req_data);
  zmq_util::send_string(req_data, &requesters[worker_address]);

  auto send_time = std::chrono::system_clock::now();
  // wait for response to actual request
  req_data = zmq_util::recv_string(&requesters[worker_address]);
  auto receive_time = std::chrono::system_clock::now();

  //cout << "request took " + to_string(chrono::duration_cast<std::chrono::microseconds>(receive_time-send_time).count()) + " microseconds\n";
}

void proxy_worker_routine(zmq::context_t* context,
    global_hash_t* global_memory_hash_ring,
    global_hash_t* global_ebs_hash_ring,
    monitoring_node_t monitoring_node,
    tbb::concurrent_unordered_map<string, shared_key_info>* placement,
    string ip,
    int thread_id) {

  address_cache_t* address_cache = new address_cache_t();


  proxy_worker_thread_t proxy_thread = proxy_worker_thread_t(ip, thread_id);

  // keep track of the keys' hotness
  unordered_map<string, size_t> key_access_frequency;
  unordered_map<string, multiset<std::chrono::time_point<std::chrono::system_clock>>> key_access_monitoring;

  // responsible for receiving user requests
  zmq::socket_t user_responder(*context, ZMQ_REP);
  user_responder.bind(proxy_thread.user_request_bind_addr_);
  // responsible for routing gossip to other tiers
  zmq::socket_t gossip_responder(*context, ZMQ_REP);
  gossip_responder.bind(proxy_thread.proxy_gossip_bind_addr_);
  // responsible for receiving benchmark request
  zmq::socket_t benchmark_puller(*context, ZMQ_PULL);
  benchmark_puller.bind(proxy_thread.banchmark_bind_addr_);

  SocketCache requesters(context, ZMQ_REQ);
  SocketCache pushers(context, ZMQ_PUSH);

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(user_responder), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(gossip_responder), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(benchmark_puller), 0, ZMQ_POLLIN, 0 },
  };

  auto hotness_start = std::chrono::system_clock::now();
  auto hotness_end = std::chrono::system_clock::now();

  int timestamp = 0;

  while (true) {
    // listen for ZMQ events
    zmq_util::poll(0, &pollitems);

    if (pollitems[0].revents & ZMQ_POLLIN) {
      // handle a user facing request
      cerr << "received user request\n";
      vector<string> v; 

      // NOTE: once we start thinking about building a programmatic API, we
      // will need a more robust form of serialization between the user & the
      // proxy & the server
      split(zmq_util::recv_string(&user_responder), ' ', v);

      if (v.size() == 0) {
          zmq_util::send_string("Empty request.\n", &user_responder);
      } else if (v[0] != "GET" && v[0] != "PUT") { 
          zmq_util::send_string("Invalid request: " + v[0] + ".\n", &user_responder);
      } else {
        bool proceed = true;
        // this data structure is for keeping track of the key value mapping in PUT request
        unordered_map<string, string> key_value_map;
        unordered_map<address_t, communication::Request> request_map;
        unordered_map<master_node_t, communication::Key_Request, node_hash> key_request_map;
        vector<string> queries;
        split(v[1], ',', queries);
        for (auto it = queries.begin(); it != queries.end(); it++) {
          string key;
          if (v[0] == "GET") {
            key = *it;
          } else {
            vector<string> kv_pair;
            split(*it, ':', kv_pair);
            key = kv_pair[0];
            // update key_value map for later value tracking
            key_value_map[key] = kv_pair[1];
          }
          // if the replication factor info is missing, query the monitoring node
          if (placement->find(key) == placement->end()) {
            zmq_util::send_string(key, &requesters[monitoring_node.replication_factor_connect_addr_]);

            string replication_factor_response = zmq_util::recv_string(&requesters[monitoring_node.replication_factor_connect_addr_]);
            communication::Replication_Factor replication_factor;
            replication_factor.ParseFromString(replication_factor_response);

            placement->emplace(std::piecewise_construct,
                               std::forward_as_tuple(key),
                               std::forward_as_tuple(replication_factor.global_memory_replication(), replication_factor.global_ebs_replication()));
          }

          // update key access monitoring map
          key_access_monitoring[key].insert(std::chrono::system_clock::now());

          vector<master_node_t> server_nodes = get_nodes(key, global_memory_hash_ring, global_ebs_hash_ring, placement);
          if (server_nodes.size() != 0) {
            // get a random node
            master_node_t server_node = server_nodes[rand() % server_nodes.size()];

            if (address_cache->find(server_node.ip_) == address_cache->end() || address_cache->at(server_node.ip_).find(key) == address_cache->at(server_node.ip_).end()) {
              // TODO: before setting the sender, check if it's already been set
              key_request_map[server_node].set_sender("proxy");
              communication::Key_Request_Tuple* tp = key_request_map[server_node].add_tuple();
              tp->set_key(key);
              tp->set_global_memory_replication(placement->find(key)->second.global_memory_replication_.load());
              tp->set_global_ebs_replication(placement->find(key)->second.global_ebs_replication_.load());
            } else {
              address_t worker_address = *(next(begin((*address_cache)[server_node.ip_][key]), rand() % (*address_cache)[server_node.ip_][key].size()));

              if (v[0] == "GET") {
                communication::Request_Get* g = request_map[worker_address].add_get();
                g->set_key(key);
              } else {
                communication::Request_Put* p = request_map[worker_address].add_put();
                p->set_key(key);
                p->set_value(key_value_map[key]);
              }
            }
          } else {
            zmq_util::send_string("No servers available.\n", &user_responder);
            proceed = false;
            break;
          }
        }

        if (proceed) {
          // loop through key request map, send key request to all nodes
          // receive key responses, and form the request map
          for (auto it = key_request_map.begin(); it != key_request_map.end(); it++) {
            // serialize request and send
            string key_req;
            it->second.SerializeToString(&key_req);
            zmq_util::send_string(key_req, &requesters[it->first.key_exchange_connect_addr_]);

            auto send_time = std::chrono::system_clock::now();
            // wait for a response from the server and deserialize
            string key_res = zmq_util::recv_string(&requesters[it->first.key_exchange_connect_addr_]);
            auto receive_time = std::chrono::system_clock::now();

            cout << "key request took " + to_string(chrono::duration_cast<std::chrono::microseconds>(receive_time-send_time).count()) + " microseconds\n";

            communication::Key_Response server_res;
            server_res.ParseFromString(key_res);

            string key;
            address_t worker_address;
            // get the worker address from the response and sent the serialized
            // data from up above to the worker thread; the reason that we do
            // this is to let the metadata thread avoid having to receive a
            // potentially large request body; since the metadata thread is
            // serial, this could potentially be a bottleneck; the current way
            // allows the metadata thread to answer lightweight requests only
            for (int i = 0; i < server_res.tuple_size(); i++) {
              key = server_res.tuple(i).key();

              // update address cache
              for (int j = 0; j < server_res.tuple(i).address_size(); j++) {
                (*address_cache)[it->first.ip_][key].insert(server_res.tuple(i).address(j).addr());
              }
              /*if (it->first.tier_ == "E") {
                // randomly choose a worker address for a key
                worker_address = server_res.tuple(i).address(rand() % server_res.tuple(i).address().size()).addr();
              } else {
                // we only have one address for memory tier
                worker_address = server_res.tuple(i).address(0).addr();
              }*/
              worker_address = *(next(begin((*address_cache)[it->first.ip_][key]), rand() % (*address_cache)[it->first.ip_][key].size()));

              cout << "worker address is " + worker_address + "\n";

              if (v[0] == "GET") {
                communication::Request_Get* g = request_map[worker_address].add_get();
                g->set_key(key);
              } else {
                communication::Request_Put* p = request_map[worker_address].add_put();
                p->set_key(key);
                p->set_value(key_value_map[key]);
              }
            }
          }

          // initialize the respond string
          string response_string = "";
          for (auto it = request_map.begin(); it != request_map.end(); it++) {
            string data;
            it->second.SerializeToString(&data);
            zmq_util::send_string(data, &requesters[it->first]);

            auto send_time = std::chrono::system_clock::now();
            // wait for response to actual request
            data = zmq_util::recv_string(&requesters[it->first]);
            auto receive_time = std::chrono::system_clock::now();

            cout << "request took " + to_string(chrono::duration_cast<std::chrono::microseconds>(receive_time-send_time).count()) + " microseconds\n";

            communication::Response response;
            response.ParseFromString(data);

            for (int i = 0; i < response.tuple_size(); i++) {
              // TODO: send a more intelligent response to the user based on the response from server
              // TODO: we should send a protobuf response that is deserialized on the proxy side... allows for a programmatic API
              if (v[0] == "GET") {
                if (response.tuple(i).succeed()) {
                  response_string += ("value of key " + response.tuple(i).key() + " is " + response.tuple(i).value() + ".\n");
                } else {
                  response_string += ("key " + response.tuple(i).key() + " does not exist\n");
                }
              } else {
                response_string += ("succeed status is " + to_string(response.tuple(i).succeed()) + " for key " + response.tuple(i).key() + "\n");
              }
            }
          }
          zmq_util::send_string(response_string, &user_responder);
        }
      }
    }

    if (pollitems[1].revents & ZMQ_POLLIN) {
      // handle gossip request
      // NOTE from Chenggang: I didn't treat the gossip as a normal user PUT because it can cause infinite loop
      cerr << "received gossip request\n";
      string key_req = zmq_util::recv_string(&gossip_responder);
      communication::Key_Request req;
      req.ParseFromString(key_req);
      string target_tier = req.target_tier();
      // this data structure is for keeping track of the mapping between each key and the workers responsible for the key
      unordered_map<string, unordered_set<address_t>> key_worker_map;
      vector<master_node_t> server_nodes;
      unordered_map<address_t, communication::Gossip> gossip_map;
      unordered_map<master_node_t, communication::Key_Request, node_hash> key_request_map;

      // loop through "req" to create the key request map for sending key address requests
      for (int i = 0; i < req.tuple_size(); i++) {
        string key = req.tuple(i).key();
        if (target_tier == "M") {
          server_nodes = get_nodes(key, global_memory_hash_ring, nullptr, placement);
        } else {
          server_nodes = get_nodes(key, nullptr, global_ebs_hash_ring, placement);
        }

        if (server_nodes.size() == 0) {
          cerr << "Error: no server node on the target tier is responsible for the key " + key + "\n";
        }

        // loop through every server node
        for (auto it = server_nodes.begin(); it != server_nodes.end(); it++) {
          // TODO: before setting the sender, check if it's already been set for efficiency
          // set the sender to "server" because the proxy is sending the key request on behalf of a server
          key_request_map[*it].set_sender("server");
          communication::Key_Request_Tuple* tp = key_request_map[*it].add_tuple();
          tp->set_key(key);
          tp->set_global_memory_replication(placement->find(key)->second.global_memory_replication_.load());
          tp->set_global_ebs_replication(placement->find(key)->second.global_ebs_replication_.load());
        }
      }

      // loop through key request map, send key request to all nodes
      // receive key responses, and form the key_worker_map
      for (auto it = key_request_map.begin(); it != key_request_map.end(); it++) {
        // serialize request and send
        string key_req;
        it->second.SerializeToString(&key_req);
        zmq_util::send_string(key_req, &requesters[it->first.key_exchange_connect_addr_]);

        // wait for a response from the server and deserialize
        string key_res = zmq_util::recv_string(&requesters[it->first.key_exchange_connect_addr_]);
        communication::Key_Response server_res;
        server_res.ParseFromString(key_res);

        string key;
        address_t worker_id;
        // get the worker address from the response and sent the serialized
        // data from up above to the worker thread; the reason that we do
        // this is to let the metadata thread avoid having to receive a
        // potentially large request body; since the metadata thread is
        // serial, this could potentially be a bottleneck; the current way
        // allows the metadata thread to answer lightweight requests only
        for (int i = 0; i < server_res.tuple_size(); i++) {
          key = server_res.tuple(i).key();
          if (it->first.tier_ == "E") {
            for (int j = 0; j < server_res.tuple(i).address_size(); j++) {
              key_worker_map[key].insert(server_res.tuple(i).address(j).addr());
            }
          } else {
            // we only have one address for memory tier
            key_worker_map[key].insert(server_res.tuple(i).address(0).addr());
          }
        }
      }

      // form the key address response
      communication::Key_Response res;
      for (auto map_it = key_worker_map.begin(); map_it != key_worker_map.end(); map_it++) {
        communication::Key_Response_Tuple* tp = res.add_tuple();
        tp->set_key(map_it->first);
        for (auto set_it = map_it->second.begin(); set_it != map_it->second.end(); set_it++) {
          communication::Key_Response_Address* ad = tp->add_address();
          ad->set_addr(*set_it);
        }
      }

      // send the key address response to the server node
      string response;
      res.SerializeToString(&response);
      zmq_util::send_string(response, &gossip_responder);
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      // handle benchmark request
      vector<string> v;
      split(zmq_util::recv_string(&benchmark_puller), ':', v);
      string type = v[0];
      string contention = v[1];
      int length = stoi(v[2]);
      int count = stoi(v[3]);

      unordered_map<string, string> key_value;
      if (contention == "H") {
        key_value[to_string(1)] = string(length, 'a');
      } else if (contention == "L") {
        for (int i = 1; i < 1000; i++) {
          key_value[to_string(i)] = string(length, 'a');
        }
      } else {
        cerr << "invalid contention\n";
      }

      // warm up
      /*for (auto it = key_value.begin(); it != key_value.end(); it++) {
        process_benchmark_request("P", it->first, it->second, context, requesters, global_memory_hash_ring, global_ebs_hash_ring, monitoring_node, placement, address_cache);
      }*/

      auto benchmark_start = std::chrono::system_clock::now();

      if (type == "G") {
        for (int i = 0; i < count; i++) {
          if (thread_id == 1) {
            cout << "processing request number " + to_string(i) + "\n";
          }
          string key = to_string(rand() % key_value.size() + 1);
          process_benchmark_request("G", key, "", context, requesters, global_memory_hash_ring, global_ebs_hash_ring, monitoring_node, placement, address_cache);
        }
      } else if (type == "P") {
        for (int i = 0; i < count; i++) {
          if (thread_id == 1) {
            cout << "processing request number " + to_string(i) + "\n";
          }
          string key = to_string(rand() % key_value.size() + 1);
          process_benchmark_request("P", key, key_value[key], context, requesters, global_memory_hash_ring, global_ebs_hash_ring, monitoring_node, placement, address_cache);
        }
      } else if (type == "M") {
        for (int i = 0; i < count/2; i++) {
          string key = to_string(rand() % key_value.size() + 1);
          process_benchmark_request("P", key, key_value[key], context, requesters, global_memory_hash_ring, global_ebs_hash_ring, monitoring_node, placement, address_cache);
          process_benchmark_request("G", key, "", context, requesters, global_memory_hash_ring, global_ebs_hash_ring, monitoring_node, placement, address_cache);
        }
      } else {
        cerr << "invalid request type\n";
      }

      auto benchmark_end = std::chrono::system_clock::now();

      auto total_time = chrono::duration_cast<std::chrono::microseconds>(benchmark_end-benchmark_start).count();

      cout << "benchamrk took " + to_string(total_time) + " microseconds\n";
      cout << "Throughput is " + to_string((double)count / (double)total_time * 1000000) + " ops/seconds\n";

      cerr << "Finished\n";
    }

    hotness_end = std::chrono::system_clock::now();

    if (chrono::duration_cast<std::chrono::seconds>(hotness_end-hotness_start).count() >= MONITORING_PERIOD) {
      timestamp++;

      for (auto map_iter = key_access_monitoring.begin(); map_iter != key_access_monitoring.end(); map_iter++) {
        string key = map_iter->first;
        auto mset = map_iter->second;

        // garbage collect key_access_monitoring
        for (auto set_iter = mset.rbegin(); set_iter != mset.rend(); set_iter++) {
          if (chrono::duration_cast<std::chrono::seconds>(hotness_end-*set_iter).count() >= MONITORING_THRESHOLD) {
            mset.erase(mset.begin(), set_iter.base());
            break;
          }
        }

        // update key_access_frequency
        key_access_frequency[key] = mset.size();
      }

      key_access_info* info = new key_access_info();
      info->thread_id = thread_id;
      info->timestamp = timestamp;
      info->key_access_frequency = key_access_frequency;
      zmq_util::send_msg((void*)info, &pushers[HOTNESS_ADDR]);
      
      hotness_start = std::chrono::system_clock::now();
    }
  }
}

// TODO: instead of cout or cerr, everything should be written to a log file.
int main(int argc, char* argv[]) {
  if (argc != 1) {
    cerr << "usage:" << argv[0] << endl;
    return 1;
  }

  string ip = get_ip("proxy");

  global_hash_t* global_memory_hash_ring = new global_hash_t();
  global_hash_t* global_ebs_hash_ring = new global_hash_t();

  // keep track of the keys' replication info
  tbb::concurrent_unordered_map<string, shared_key_info>* placement = new tbb::concurrent_unordered_map<string, shared_key_info>();

  map<int, unordered_set<int>> proxy_thread_report;
  map<int, unordered_map<string, size_t>> timestamp_key_access_frequency;
 
  // read in the initial server addresses and build the hash ring
  string ip_line;
  ifstream address;
  // read existing memory servers and populate the memory hash ring
  address.open("conf/proxy/existing_memory_servers.txt");

  memory_lock.lock();
  while (getline(address, ip_line)) {
    cerr << ip_line << "\n";
    global_memory_hash_ring->insert(master_node_t(ip_line, "M"));
  }
  memory_lock.unlock();
  address.close();

  // read existing ebs servers and populate the ebs hash ring
  address.open("conf/proxy/existing_ebs_servers.txt");

  ebs_lock.lock();
  while (getline(address, ip_line)) {
    cerr << ip_line << "\n";
    global_ebs_hash_ring->insert(master_node_t(ip_line, "E"));
  }
  ebs_lock.unlock();
  address.close();

  vector<address_t> monitoring_address;

  // read existing monitoring nodes
  address.open("conf/proxy/monitoring_address.txt");

  while (getline(address, ip_line)) {
    cerr << ip_line << "\n";
    monitoring_address.push_back(ip_line);
  }
  address.close();

  monitoring_node_t monitoring_node = monitoring_node_t(*monitoring_address.begin());


  zmq::context_t context(1);

  SocketCache pushers(&context, ZMQ_PUSH);

  vector<thread> proxy_worker_threads;

  for (int thread_id = 1; thread_id <= PROXY_THREAD_NUM; thread_id++) {
    proxy_worker_threads.push_back(thread(proxy_worker_routine, &context, global_memory_hash_ring, global_ebs_hash_ring, monitoring_node, placement, ip, thread_id));
  }

  // responsible for both node join and departure
  zmq::socket_t join_puller(context, ZMQ_PULL);
  join_puller.bind(NOTIFY_BIND_ADDR);

  // responsible for handling key replication factor change requests from server nodes
  zmq::socket_t replication_factor_change_puller(context, ZMQ_PULL);
  replication_factor_change_puller.bind(REPLICATION_FACTOR_BIND_ADDR);

  // responsible for accepting key hotness info from worker threads
  zmq::socket_t key_hotness_puller(context, ZMQ_PULL);
  key_hotness_puller.bind(HOTNESS_ADDR);

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(replication_factor_change_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(key_hotness_puller), 0, ZMQ_POLLIN, 0 }
  };

  while (true) {
    // listen for ZMQ events
    zmq_util::poll(-1, &pollitems);

    // handle a join or depart event coming from the server side
    if (pollitems[0].revents & ZMQ_POLLIN) {
      vector<string> v;
      split(zmq_util::recv_string(&join_puller), ':', v);
      if (v[0] == "join") {
        cerr << "received join\n";
        // update hash ring
        if (v[1] == "M") {
          memory_lock.lock();
          global_memory_hash_ring->insert(master_node_t(v[2], "M"));
          memory_lock.unlock();
        } else if (v[1] == "E") {
          ebs_lock.lock();
          global_ebs_hash_ring->insert(master_node_t(v[2], "E"));
          ebs_lock.unlock();
        } else {
          cerr << "Invalid Tier info\n";
        }

        memory_lock.lock_shared();
        ebs_lock.lock_shared();
        cerr << "memory hash ring size is " + to_string(global_memory_hash_ring->size()) + "\n";
        cerr << "ebs hash ring size is " + to_string(global_ebs_hash_ring->size()) + "\n";
        memory_lock.unlock_shared();
        ebs_lock.unlock_shared();
      } else if (v[0] == "depart") {
        cerr << "received depart\n";
        // update hash ring
        if (v[1] == "M") {
          memory_lock.lock();
          global_memory_hash_ring->erase(master_node_t(v[2], "M"));
          memory_lock.unlock();
        } else if (v[1] == "E") {
          ebs_lock.lock();
          global_ebs_hash_ring->erase(master_node_t(v[2], "E"));
          ebs_lock.unlock();
        } else {
          cerr << "Invalid Tier info\n";
        }

        memory_lock.lock_shared();
        ebs_lock.lock_shared();
        cerr << "memory hash ring size is " + to_string(global_memory_hash_ring->size()) + "\n";
        cerr << "ebs hash ring size is " + to_string(global_ebs_hash_ring->size()) + "\n";
        memory_lock.unlock_shared();
        ebs_lock.unlock_shared();
      }
    }

    if (pollitems[1].revents & ZMQ_POLLIN) {
      cerr << "received replication factor change request\n";
      string placement_req = zmq_util::recv_string(&replication_factor_change_puller);
      communication::Replication_Factor_Request req;
      req.ParseFromString(placement_req);

      // update the placement info
      for (int i = 0; i < req.tuple_size(); i++) {
        string key = req.tuple(i).key();
        // update the placement map
        placement->find(key)->second.global_memory_replication_.store(req.tuple(i).global_memory_replication());
        placement->find(key)->second.global_ebs_replication_.store(req.tuple(i).global_ebs_replication());
      }
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      //cerr << "received key hotness update\n";
      zmq::message_t msg;
      zmq_util::recv_msg(&key_hotness_puller, msg);
      key_access_info* info = *(key_access_info **)(msg.data());
      proxy_thread_report[info->timestamp].insert(info->thread_id);
      for (auto it = info->key_access_frequency.begin(); it != info->key_access_frequency.end(); it++) {
        if (timestamp_key_access_frequency[info->timestamp].find(it->first) == timestamp_key_access_frequency[info->timestamp].end()) {
          timestamp_key_access_frequency[info->timestamp][it->first] = it->second;
        } else {
          timestamp_key_access_frequency[info->timestamp][it->first] += it->second;
        }
      }
      delete info;

      unordered_set<int> ts_to_remove;
      for (auto it = proxy_thread_report.begin(); it != proxy_thread_report.end(); it++) {
        if (it->second.size() == PROXY_THREAD_NUM) {
          ts_to_remove.insert(it->first);
          // send hotness info to the monitoring node
          communication::Key_Hotness_Update khu;
          khu.set_node_ip(ip);
          for (auto iter = timestamp_key_access_frequency[it->first].begin(); iter != timestamp_key_access_frequency[it->first].end(); iter++) {
            communication::Key_Hotness_Update_Tuple* tp = khu.add_tuple();
            tp->set_key(iter->first);
            tp->set_access(iter->second);
          }
          string msg;
          khu.SerializeToString(&msg);
          zmq_util::send_string(msg, &pushers[monitoring_node.key_hotness_connect_addr_]);
        }
      }

      for (auto it = ts_to_remove.begin(); it != ts_to_remove.end(); it++) {
        proxy_thread_report.erase(*it);
        timestamp_key_access_frequency.erase(*it);
      }
    }
  }
}
