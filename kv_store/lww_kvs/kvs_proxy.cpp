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
#include "rc_kv_store.h"
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"
#include "consistent_hash_map.hpp"
#include "common.h"

using namespace std;
using address_t = string;

#define DEFAULT_GLOBAL_MEMORY_REPLICATION 1
#define DEFAULT_GLOBAL_EBS_REPLICATION 2

#define MONITORING_THRESHOLD 30
#define MONITORING_PERIOD 3

// given a key, check memory and ebs hash ring to find all the server nodes responsible for the key
vector<master_node_t> get_nodes(string key, global_hash_t* global_memory_hash_ring, global_hash_t* global_ebs_hash_ring, unordered_map<string, key_info>& placement) {
  vector<master_node_t> server_nodes;
  // use hash ring to find the right node to contact
  // first, look up the memory hash ring
  if (global_memory_hash_ring != nullptr) {
    auto it = global_memory_hash_ring->find(key);
    if (it != global_memory_hash_ring->end()) {
      for (int i = 0; i < placement[key].global_memory_replication_; i++) {
        server_nodes.push_back(it->second);
        if (++it == global_memory_hash_ring->end()) {
          it = global_memory_hash_ring->begin();
        }
      }
    }
  }
  // then check the ebs hash ring
  if (global_ebs_hash_ring != nullptr) {
    auto it = global_ebs_hash_ring->find(key);
    if (it != global_ebs_hash_ring->end()) {
      for (int i = 0; i < placement[key].global_ebs_replication_; i++) {
        server_nodes.push_back(it->second);
        if (++it == global_ebs_hash_ring->end()) {
          it = global_ebs_hash_ring->begin();
        }
      }
    }  
  }
  return server_nodes;
}

// TODO: instead of cout or cerr, everything should be written to a log file.
int main(int argc, char* argv[]) {
  if (argc != 1) {
    cerr << "usage:" << argv[0] << endl;
    return 1;
  }

  string ip = getIP();

  global_hash_t global_memory_hash_ring;
  global_hash_t global_ebs_hash_ring;

  // keep track of the keys' replication info
  unordered_map<string, key_info> placement;

  // keep track of the keys' hotness
  unordered_map<string, size_t> key_access_frequency;
  unordered_map<string, multiset<std::chrono::time_point<std::chrono::system_clock>>> key_access_monitoring;

  // keep track of memory tier storage consumption
  unordered_map<master_node_t, size_t, node_hash> memory_tier_storage;

  // keep track of ebs tier storage consumption
  unordered_map<master_node_t, unordered_map<string, size_t>, node_hash> ebs_tier_storage;
 
  // read in the initial server addresses and build the hash ring
  string ip_line;
  ifstream address;
  // read existing memory servers and populate the memory hash ring
  address.open("conf/proxy/existing_memory_servers.txt");

  while (getline(address, ip_line)) {
    cerr << ip_line << "\n";
    global_memory_hash_ring.insert(master_node_t(ip_line, "M"));
  }
  address.close();

  // read existing ebs servers and populate the ebs hash ring
  address.open("conf/proxy/existing_ebs_servers.txt");

  while (getline(address, ip_line)) {
    cerr << ip_line << "\n";
    global_ebs_hash_ring.insert(master_node_t(ip_line, "E"));
  }
  address.close();


  zmq::context_t context(1);
  SocketCache requesters(&context, ZMQ_REQ);
  SocketCache pushers(&context, ZMQ_PUSH);

  // responsible for both node join and departure
  zmq::socket_t join_puller(context, ZMQ_PULL);
  join_puller.bind(PROXY_NOTIFY_BIND_ADDR);
  // responsible for receiving user requests
  zmq::socket_t user_responder(context, ZMQ_REP);
  user_responder.bind(PROXY_CONTACT_BIND_ADDR);
  // responsible for routing gossip to other tiers
  zmq::socket_t gossip_responder(context, ZMQ_REP);
  gossip_responder.bind(PROXY_GOSSIP_BIND_ADDR);
  // responsible for handling key replication factor change requests from server nodes
  zmq::socket_t replication_factor_change_puller(context, ZMQ_PULL);
  replication_factor_change_puller.bind(PROXY_PLACEMENT_BIND_ADDR);
  // responsible for receiving storage consumption updates
  zmq::socket_t storage_consumption_puller(context, ZMQ_PULL);
  storage_consumption_puller.bind(PROXY_STORAGE_CONSUMPTION_BIND_ADDR);

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(user_responder), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(gossip_responder), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(replication_factor_change_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(storage_consumption_puller), 0, ZMQ_POLLIN, 0 }
  };

  auto start = std::chrono::system_clock::now();
  auto end = std::chrono::system_clock::now();

  while (true) {
    // listen for ZMQ events
    zmq_util::poll(0, &pollitems);

    // handle a join or depart event coming from the server side
    if (pollitems[0].revents & ZMQ_POLLIN) {
      vector<string> v;
      split(zmq_util::recv_string(&join_puller), ':', v);
      if (v[0] == "join") {
        cerr << "received join\n";
        // update hash ring
        if (v[1] == "M") {
          global_memory_hash_ring.insert(master_node_t(v[2], "M"));
        } else if (v[1] == "E") {
          global_ebs_hash_ring.insert(master_node_t(v[2], "E"));
        } else {
          cerr << "Invalid Tier info\n";
        }
        cerr << "memory hash ring size is " + to_string(global_memory_hash_ring.size()) + "\n";
        cerr << "ebs hash ring size is " + to_string(global_ebs_hash_ring.size()) + "\n";
      } else if (v[0] == "depart") {
        cerr << "received depart\n";
        // update hash ring
        if (v[1] == "M") {
          global_memory_hash_ring.erase(master_node_t(v[2], "M"));
          memory_tier_storage.erase(master_node_t(v[2], "M"));
        } else if (v[1] == "E") {
          global_ebs_hash_ring.erase(master_node_t(v[2], "E"));
          ebs_tier_storage.erase(master_node_t(v[2], "E"));
        } else {
          cerr << "Invalid Tier info\n";
        }
        cerr << "memory hash ring size is " + to_string(global_memory_hash_ring.size()) + "\n";
        cerr << "ebs hash ring size is " + to_string(global_ebs_hash_ring.size()) + "\n";
      }
    }

    if (pollitems[1].revents & ZMQ_POLLIN) {
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
          // set the key info for this key (using the default replication factor for now)
          if (placement.find(key) == placement.end()) {
            placement[key] = key_info(DEFAULT_GLOBAL_MEMORY_REPLICATION, DEFAULT_GLOBAL_EBS_REPLICATION);
          }

          // update key access monitoring map
          key_access_monitoring[key].insert(std::chrono::system_clock::now());

          vector<master_node_t> server_nodes = get_nodes(key, &global_memory_hash_ring, &global_ebs_hash_ring, placement);
          if (server_nodes.size() != 0) {
            // randomly choose a server node responsible for this key and update the key request map
            master_node_t server_node = server_nodes[rand() % server_nodes.size()];
            // TODO: before setting the sender, check if it's already been set
            key_request_map[server_node].set_sender("proxy");
            communication::Key_Request_Tuple* tp = key_request_map[server_node].add_tuple();
            tp->set_key(key);
            tp->set_global_memory_replication(placement[key].global_memory_replication_);
            tp->set_global_ebs_replication(placement[key].global_ebs_replication_);
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

            // wait for a response from the server and deserialize
            string key_res = zmq_util::recv_string(&requesters[it->first.key_exchange_connect_addr_]);
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
              if (it->first.tier_ == "E") {
                // randomly choose a worker address for a key
                worker_address = server_res.tuple(i).address(rand() % server_res.tuple(i).address().size()).addr();
              } else {
                // we only have one address for memory tier
                worker_address = server_res.tuple(i).address(0).addr();
              }

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
            // wait for response to actual request
            data = zmq_util::recv_string(&requesters[it->first]);
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

    if (pollitems[2].revents & ZMQ_POLLIN) {
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
          server_nodes = get_nodes(key, &global_memory_hash_ring, nullptr, placement);
        } else {
          server_nodes = get_nodes(key, nullptr, &global_ebs_hash_ring, placement);
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
          tp->set_global_memory_replication(placement[key].global_memory_replication_);
          tp->set_global_ebs_replication(placement[key].global_ebs_replication_);
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

    if (pollitems[3].revents & ZMQ_POLLIN) {
      cerr << "received replication factor change request\n";
      string placement_req = zmq_util::recv_string(&replication_factor_change_puller);
      communication::Placement_Request req;
      req.ParseFromString(placement_req);

      // used to keep track of the original replication factors for the requested keys
      unordered_map<string, pair<int, int>> orig_placement_info;

      // used to keep track of the key value mapping
      unordered_map<string, pair<string, int>> key_value_map;
      unordered_map<address_t, communication::Request> request_map;
      unordered_map<master_node_t, communication::Key_Request, node_hash> key_request_map;

      for (int i = 0; i < req.tuple_size(); i++) {
        string key = req.tuple(i).key();

        vector<master_node_t> server_nodes = get_nodes(key, &global_memory_hash_ring, &global_ebs_hash_ring, placement);

        // randomly choose a server node responsible for this key and update the key request map
        // assume the size of server_nodes is not 0 (it shouldn't be) 
        master_node_t server_node = server_nodes[rand() % server_nodes.size()];
        // TODO: before setting the sender, check if it's already been set
        key_request_map[server_node].set_sender("proxy");
        communication::Key_Request_Tuple* tp = key_request_map[server_node].add_tuple();
        tp->set_key(key);
        tp->set_global_memory_replication(placement[key].global_memory_replication_);
        tp->set_global_ebs_replication(placement[key].global_ebs_replication_);
      }

      // loop through key request map, send key request to all nodes
      // receive key responses, and form the request map
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
        address_t worker_address;
        // get the worker address from the response and sent the serialized
        // data from up above to the worker thread; the reason that we do
        // this is to let the metadata thread avoid having to receive a
        // potentially large request body; since the metadata thread is
        // serial, this could potentially be a bottleneck; the current way
        // allows the metadata thread to answer lightweight requests only
        for (int i = 0; i < server_res.tuple_size(); i++) {
          key = server_res.tuple(i).key();
          if (it->first.tier_ == "E") {
            // randomly choose a worker address for a key
            worker_address = server_res.tuple(i).address(rand() % server_res.tuple(i).address().size()).addr();
          } else {
            // we only have one address for memory tier
            worker_address = server_res.tuple(i).address(0).addr();
          }

          communication::Request_Get* g = request_map[worker_address].add_get();
          g->set_key(key);
        }
      }

      for (auto it = request_map.begin(); it != request_map.end(); it++) {
        string data;
        it->second.SerializeToString(&data);
        zmq_util::send_string(data, &requesters[it->first]);
        // wait for response to actual request
        data = zmq_util::recv_string(&requesters[it->first]);
        communication::Response response;
        response.ParseFromString(data);

        // populate key value map
        for (int i = 0; i < response.tuple_size(); i++) {
          key_value_map[response.tuple(i).key()] = pair<string, int>(response.tuple(i).value(), response.tuple(i).timestamp());
        }
      }

      // update the placement info
      for (int i = 0; i < req.tuple_size(); i++) {
        string key = req.tuple(i).key();
        orig_placement_info[key] = pair<int, int>(placement[key].global_memory_replication_, placement[key].global_ebs_replication_);
        // update the placement map
        placement[key].global_memory_replication_ = req.tuple(i).global_memory_replication();
        placement[key].global_ebs_replication_ = req.tuple(i).global_ebs_replication();
      }


      // form the placement request map
      unordered_map<address_t, communication::Placement_Request> placement_request_map;

      for (int i = 0; i < req.tuple_size(); i++) {
        string key = req.tuple(i).key();
        // for each tier, take the max between the key's original rep factor and current rep factor
        int memory_rep = max(placement[key].global_memory_replication_, orig_placement_info[key].first);
        int ebs_rep = max(placement[key].global_ebs_replication_, orig_placement_info[key].second);

        // form placement requests for memory tier nodes
        auto it = global_memory_hash_ring.find(key);
        if (it != global_memory_hash_ring.end()) {
          for (int i = 0; i < memory_rep; i++) {
            communication::Placement_Request_Tuple* tp = placement_request_map[it->second.node_placement_connect_addr_].add_tuple();
            tp->set_key(key);
            tp->set_global_memory_replication(placement[key].global_memory_replication_);
            tp->set_global_ebs_replication(placement[key].global_ebs_replication_);

            if (++it == global_memory_hash_ring.end()) {
              it = global_memory_hash_ring.begin();
            }
          }
        }

        // form placement requests for ebs tier nodes
        it = global_ebs_hash_ring.find(key);
        if (it != global_ebs_hash_ring.end()) {
          for (int i = 0; i < ebs_rep; i++) {
            communication::Placement_Request_Tuple* tp = placement_request_map[it->second.node_placement_connect_addr_].add_tuple();
            tp->set_key(key);
            tp->set_global_memory_replication(placement[key].global_memory_replication_);
            tp->set_global_ebs_replication(placement[key].global_ebs_replication_);

            if (++it == global_ebs_hash_ring.end()) {
              it = global_ebs_hash_ring.begin();
            }
          }
        }
      }

      // send placement info update to all relevant server nodes
      for (auto it = placement_request_map.begin(); it != placement_request_map.end(); it++) {
        string data;
        it->second.SerializeToString(&data);
        zmq_util::send_string(data, &pushers[it->first]);
      }
    }

    if (pollitems[4].revents & ZMQ_POLLIN) {
      cerr << "received storage update\n";
      string storage_msg = zmq_util::recv_string(&storage_consumption_puller);
      communication::Storage_Update su;
      su.ParseFromString(storage_msg);
      if (su.node_type() == "M") {
        memory_tier_storage[master_node_t(su.node_ip(), "M")] = su.memory_storage();
      } else {
        ebs_tier_storage[master_node_t(su.node_ip(), "E")].clear();
        
        for (int i = 0; i < su.ebs_size(); i++) {
          ebs_tier_storage[master_node_t(su.node_ip(), "E")][su.ebs(i).id()] = su.ebs(i).storage();
        }
      }
    }

    end = std::chrono::system_clock::now();
    if (chrono::duration_cast<std::chrono::seconds>(end-start).count() >= MONITORING_PERIOD) {
      for (auto map_iter = key_access_monitoring.begin(); map_iter != key_access_monitoring.end(); map_iter++) {
        string key = map_iter->first;
        auto mset = map_iter->second;

        // garbage collect key_access_monitoring
        for (auto set_iter = mset.rbegin(); set_iter != mset.rend(); set_iter++) {
          if (chrono::duration_cast<std::chrono::seconds>(end-*set_iter).count() >= MONITORING_THRESHOLD) {
            mset.erase(mset.begin(), set_iter.base());
            break;
          }
        }

        // update key_access_frequency
        key_access_frequency[key] = mset.size();
      }
      
      start = std::chrono::system_clock::now();
    }

  }
}
