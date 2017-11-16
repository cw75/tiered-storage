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

//#define GetCurrentDir getcwd

using namespace std;

// If the total number of updates to the kvs before the last gossip reaches THRESHOLD, then the thread gossips to others.
#define THRESHOLD 1

// Define the gossip period (frequency)
#define PERIOD 5

// Define the number of memory threads
#define MEMORY_THREAD_NUM 3

// TODO: reconsider type names here
typedef Concurrent_KV_Store<string, RC_KVS_PairLattice<string>> Database;

typedef unordered_map<string, RC_KVS_PairLattice<string>> gossip_data;

typedef pair<size_t, unordered_set<string>> changeset_data;

typedef unordered_map<string, unordered_set<string>> changeset_address;

typedef unordered_map<string, unordered_set<pair<string, bool>, pair_hash>> redistribution_address;

// TODO:  this should be changed to something more globally robust
atomic<int> lww_timestamp(0);

pair<RC_KVS_PairLattice<string>, bool> process_memory_get(string key, Database* kvs) {
  bool succeed;
  auto res = kvs->get(key, succeed);
  // check if the value is an empty string
  if (res.reveal().value == "") {
    succeed = false;
  }
  return pair<RC_KVS_PairLattice<string>, bool>(res, succeed);
}

bool process_memory_put(string key, int timestamp, string value, Database* kvs) {
  timestamp_value_pair<string> p = timestamp_value_pair<string>(timestamp, value);
  bool succeed;
  kvs->put(key, RC_KVS_PairLattice<string>(p), succeed);
  return succeed;
}

// remove in memory tier is implemented as replacing the value with an empty string
// because concurrent unordered map's erase is not thread safe
void process_memory_remove(string key, Database* kvs) {
  bool succeed;
  auto res = kvs->get(key, succeed);
  timestamp_value_pair<string> p = timestamp_value_pair<string>(res.reveal().timestamp, "");
  kvs->put(key, RC_KVS_PairLattice<string>(p), succeed);
}

// Handle request from proxies
string process_proxy_request(communication::Request& req, int thread_id, unordered_set<string>& local_changeset, Database* kvs) {
  communication::Response response;

  if (req.get_size() != 0) {
    cout << "received get by thread " << thread_id << "\n";
    for (int i = 0; i < req.get_size(); i++) {
      auto res = process_memory_get(req.get(i).key(), kvs);
      communication::Response_Tuple* tp = response.add_tuple();
      tp->set_key(req.get(i).key());
      tp->set_value(res.first.reveal().value);
      tp->set_timestamp(res.first.reveal().timestamp);
      tp->set_succeed(res.second);
    }
  } else if (req.put_size() != 0) {
    cout << "received put by thread " << thread_id << "\n";
    for (int i = 0; i < req.put_size(); i++) {
      bool succeed = process_memory_put(req.put(i).key(), lww_timestamp.load(), req.put(i).value(), kvs);
      communication::Response_Tuple* tp = response.add_tuple();
      tp->set_key(req.put(i).key());
      tp->set_succeed(succeed);
      local_changeset.insert(req.put(i).key());
    }
  }
  
  string data;
  response.SerializeToString(&data);
  return data;
}

// Handle distributed gossip from threads on other nodes
void process_distributed_gossip(communication::Gossip& gossip, int thread_id, Database* kvs) {
  for (int i = 0; i < gossip.tuple_size(); i++) {
    process_memory_put(gossip.tuple(i).key(), gossip.tuple(i).timestamp(), gossip.tuple(i).value(), kvs);
  }
}

void send_gossip(changeset_address* change_set_addr, SocketCache& cache, string ip, int thread_id, Database* kvs) {
  unordered_map<string, communication::Gossip> distributed_gossip_map;

  for (auto map_it = change_set_addr->begin(); map_it != change_set_addr->end(); map_it++) {
    vector<string> v;
    split(map_it->first, ':', v);
    string gossip_addr = worker_node_t(v[0], stoi(v[1]) - SERVER_PORT).distributed_gossip_connect_addr_;

    // add to distribute gossip map
    for (auto set_it = map_it->second.begin(); set_it != map_it->second.end(); set_it++) {
      auto res = process_memory_get(*set_it, kvs);
      // if the GET is successful
      if (res.second) {
        cout << "distribute gossip key: " + *set_it + " by thread " + to_string(thread_id) + "\n";
        communication::Gossip_Tuple* tp = distributed_gossip_map[gossip_addr].add_tuple();
        tp->set_key(*set_it);
        tp->set_value(res.first.reveal().value);
        tp->set_timestamp(res.first.reveal().timestamp);
      }
    }
  }
  // send distributed gossip
  for (auto it = distributed_gossip_map.begin(); it != distributed_gossip_map.end(); it++) {
    string data;
    it->second.SerializeToString(&data);
    zmq_util::send_string(data, &cache[it->first]);
  }
}

// memory worker event loop
void memory_worker_routine (zmq::context_t* context, Database* kvs, string ip, int thread_id)
{
  size_t port = SERVER_PORT + thread_id;

  unordered_set<string> local_changeset;
  worker_node_t wnode = worker_node_t(ip, thread_id);

  // socket that respond to proxy requests
  zmq::socket_t responder(*context, ZMQ_REP);
  responder.bind(wnode.proxy_connection_bind_addr_);
  // socket that listens for distributed gossip
  zmq::socket_t dgossip_puller(*context, ZMQ_PULL);
  dgossip_puller.bind(wnode.distributed_gossip_bind_addr_);
  // socket that listens for redistribution
  zmq::socket_t lredistribute_puller(*context, ZMQ_PULL);
  lredistribute_puller.bind(wnode.local_redistribute_addr_);

  // used to communicate with master thread for changeset addresses
  zmq::socket_t changeset_address_requester(*context, ZMQ_REQ);
  changeset_address_requester.connect(CHANGESET_ADDR);

  // used to send gossip
  SocketCache cache(context, ZMQ_PUSH);

  //  Initialize poll set
  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(responder), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(dgossip_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(lredistribute_puller), 0, ZMQ_POLLIN, 0 }
  };

  auto start = std::chrono::system_clock::now();
  auto end = std::chrono::system_clock::now();

  // Enter the event loop
  while (true) {
    zmq_util::poll(0, &pollitems);

    // If there is a request from proxies
    if (pollitems[0].revents & ZMQ_POLLIN) {
      cout << "received a request from the proxy by thread " + to_string(thread_id) + "\n";
      string data = zmq_util::recv_string(&responder);
      communication::Request req;
      req.ParseFromString(data);
      //  Process request
      string result = process_proxy_request(req, thread_id, local_changeset, kvs);
      //  Send reply back to proxy
      zmq_util::send_string(result, &responder);
    }

    // If there is gossip from threads on other nodes
    if (pollitems[1].revents & ZMQ_POLLIN) {
      cout << "received distributed gossip by thread " + to_string(thread_id) + "\n";
      string data = zmq_util::recv_string(&dgossip_puller);
      communication::Gossip gossip;
      gossip.ParseFromString(data);
      //  Process distributed gossip
      process_distributed_gossip(gossip, thread_id, kvs);
    }

    // If receives a local redistribute command
    if (pollitems[2].revents & ZMQ_POLLIN) {
      cout << "received local redistribute request by thread " + to_string(thread_id) + "\n";
      zmq::message_t msg;
      zmq_util::recv_msg(&lredistribute_puller, msg);
      redistribution_address* r_data = *(redistribution_address **)(msg.data());
      changeset_address c_address;
      unordered_set<string> remove_set;
      for (auto map_it = r_data->begin(); map_it != r_data->end(); map_it++) {
        for (auto set_it = map_it->second.begin(); set_it != map_it->second.end(); set_it++) {
          c_address[map_it->first].insert(set_it->first);
          if (set_it->second) {
            remove_set.insert(set_it->first);
          }
        }
      }
      send_gossip(&c_address, cache, ip, thread_id, kvs);
      delete r_data;
      // remove keys in the remove set
      for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
        process_memory_remove(*it, kvs);
      }
    }

    end = std::chrono::system_clock::now();
    if (chrono::duration_cast<std::chrono::seconds>(end-start).count() >= PERIOD || local_changeset.size() >= THRESHOLD) {
      if (local_changeset.size() >= THRESHOLD) {
        cout << "reached gossip threshold\n";
      }
      if (local_changeset.size() > 0) {
        changeset_data* data = new changeset_data();
        data->first = port;
        for (auto it = local_changeset.begin(); it != local_changeset.end(); it++) {
          (data->second).insert(*it);
        }
        zmq_util::send_msg((void*)data, &changeset_address_requester);
        zmq::message_t msg;
        zmq_util::recv_msg(&changeset_address_requester, msg);
        changeset_address* res = *(changeset_address **)(msg.data());
        send_gossip(res, cache, ip, thread_id, kvs);
        delete res;
        local_changeset.clear();
      }
      start = std::chrono::system_clock::now();
    }
  }
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    cerr << "usage:" << argv[0] << " <new_node>" << endl;
    return 1;
  }
  if (string(argv[1]) != "y" && string(argv[1]) != "n") {
    cerr << "invalid argument" << endl;
    return 1;
  }

  Database* kvs = new Database();

  string ip = getIP();
  string new_node = argv[1];

  master_node_t mnode = master_node_t(ip, "M");

  //  Prepare our context
  zmq::context_t context(1);

  SocketCache cache(&context, ZMQ_PUSH);

  SocketCache key_address_requesters(&context, ZMQ_REQ);

  global_hash_t global_memory_hash_ring;

  unordered_map<string, key_info> placement;

  //unordered_map<string, unordered_map<string, unordered_set<string>>> change_map;

  vector<string> proxy_address;

  // read proxy address from the file
  string ip_line;
  ifstream address;
  address.open("conf/server/proxy_address.txt");
  while (getline(address, ip_line)) {
    proxy_address.push_back(ip_line);
  }
  address.close();

  // read server address from the file
  if (new_node == "n") {
    address.open("conf/server/start_servers.txt");
    // add all other servers
    while (getline(address, ip_line)) {
      global_memory_hash_ring.insert(master_node_t(ip_line, "M"));
    }
    address.close();
   
    // add yourself to the ring
    global_memory_hash_ring.insert(mnode);
  }

  // get server address from the seed node
  else {
    address.open("conf/server/seed_server.txt");
    getline(address, ip_line);       
    address.close();
    zmq::socket_t addr_requester(context, ZMQ_REQ);
    addr_requester.connect(master_node_t(ip_line, "M").seed_connection_connect_addr_);
    zmq_util::send_string("join", &addr_requester);
    vector<string> addresses;
    split(zmq_util::recv_string(&addr_requester), '|', addresses);
    for (auto it = addresses.begin(); it != addresses.end(); it++) {
      global_memory_hash_ring.insert(master_node_t(*it, "M"));
    }
    // add itself to global hash ring
    global_memory_hash_ring.insert(mnode);
  }

  for (auto it = global_memory_hash_ring.begin(); it != global_memory_hash_ring.end(); it++) {
    cout << "address is " + it->second.ip_ + "\n";
  }


  vector<thread> memory_threads;

  for (int thread_id = 1; thread_id <= MEMORY_THREAD_NUM; thread_id++) {
    memory_threads.push_back(thread(memory_worker_routine, &context, kvs, ip, thread_id));
  }

  if (new_node == "y") {
    // notify other servers
    for (auto it = global_memory_hash_ring.begin(); it != global_memory_hash_ring.end(); it++) {
      if (it->second.ip_.compare(ip) != 0) {
        zmq_util::send_string(ip, &cache[(it->second).node_join_connect_addr_]);
      }
    }
  }

  // notify proxies
  for (auto it = proxy_address.begin(); it != proxy_address.end(); it++) {
    zmq_util::send_string("join:M:" + ip, &cache[master_node_t(*it, "M").proxy_notify_connect_addr_]);
  }

  // (seed node) responsible for sending the server address to the new node
  zmq::socket_t addr_responder(context, ZMQ_REP);
  addr_responder.bind(SEED_BIND_ADDR);

  // listens for node joining
  zmq::socket_t join_puller(context, ZMQ_PULL);
  join_puller.bind(NODE_JOIN_BIND_ADDR);

  // listens for node departing
  zmq::socket_t depart_puller(context, ZMQ_PULL);
  depart_puller.bind(NODE_DEPART_BIND_ADDR);

  // responsible for sending the worker address (responsible for the requested key) to the proxy or other servers
  zmq::socket_t key_address_responder(context, ZMQ_REP);
  key_address_responder.bind(KEY_EXCHANGE_BIND_ADDR);

  // responsible for responding changeset addresses from workers
  zmq::socket_t changeset_address_responder(context, ZMQ_REP);
  changeset_address_responder.bind(CHANGESET_ADDR);

  // responsible for listening for a command that this node should leave
  zmq::socket_t self_depart_responder(context, ZMQ_REP);
  self_depart_responder.bind(SELF_DEPART_BIND_ADDR);

  // responsible for listening for key replication factor change
  zmq::socket_t replication_factor_change_puller(context, ZMQ_PULL);
  replication_factor_change_puller.bind(NODE_PLACEMENT_BIND_ADDR);

  // set up zmq receivers
  vector<zmq::pollitem_t> pollitems = {{static_cast<void*>(addr_responder), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(join_puller), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(depart_puller), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(key_address_responder), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(changeset_address_responder), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(self_depart_responder), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(replication_factor_change_puller), 0, ZMQ_POLLIN, 0}
                                     };

  auto start = std::chrono::system_clock::now();
  auto end = std::chrono::system_clock::now();

  while (true) {
    zmq_util::poll(0, &pollitems);
    if (pollitems[0].revents & ZMQ_POLLIN) {
      string request = zmq_util::recv_string(&addr_responder);
      cout << "request is " + request + "\n";
      if (request == "join") {
        string addresses;
        for (auto it = global_memory_hash_ring.begin(); it != global_memory_hash_ring.end(); it++) {
          addresses += (it->second.ip_ + "|");
        }
        addresses.pop_back();
        zmq_util::send_string(addresses, &addr_responder);
      }
      else {
        cout << "invalid request\n";
      }
    }

    if (pollitems[1].revents & ZMQ_POLLIN) {
      string new_server_ip = zmq_util::recv_string(&join_puller);
      cout << "Received a node join. New node is " << new_server_ip << ".\n";
      master_node_t new_node = master_node_t(new_server_ip, "M");

      // update hash ring
      global_memory_hash_ring.insert(new_node);
      // instruct a random worker to send gossip to the new server! (2 phase)
      int tid = 1 + rand()%MEMORY_THREAD_NUM;
      string worker_address = worker_node_t(ip, tid).local_redistribute_addr_;
      unordered_set<string> key_to_query;

      for (auto it = placement.begin(); it != placement.end(); it++) {
        auto result = responsible<master_node_t, crc32_hasher>(it->first, it->second.global_memory_replication_, global_memory_hash_ring, new_node.id_);
        if (result.first && (result.second->ip_.compare(ip) == 0)) {
          key_to_query.insert(it->first);
        }
      }

      communication::Key_Response resp = get_key_address<key_info>(new_node.key_exchange_connect_addr_, "", key_to_query, key_address_requesters, placement);

      redistribution_address* r_address = new redistribution_address();
      // for each key in the response
      for (int i = 0; i < resp.tuple_size(); i++) {
        string key = resp.tuple(i).key();
        // we only have one address for memory tier
        string target_address = resp.tuple(i).address(0).addr();
        (*r_address)[target_address].insert(pair<string, bool>(key, true));
      }

      zmq_util::send_msg((void*)r_address, &cache[worker_address]);
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      cout << "received departure of other nodes\n";
      string departing_server_ip = zmq_util::recv_string(&depart_puller);
      // update hash ring
      global_memory_hash_ring.erase(master_node_t(departing_server_ip, "M"));
    }

    if (pollitems[3].revents & ZMQ_POLLIN) {
      cout << "received key address request\n";
      lww_timestamp++;
      string key_req = zmq_util::recv_string(&key_address_responder);
      communication::Key_Request req;
      req.ParseFromString(key_req);
      string sender = req.sender();
      communication::Key_Response res;

      // for every requested key
      for (int i = 0; i < req.tuple_size(); i++) {
        string key = req.tuple(i).key();
        int gmr = req.tuple(i).global_memory_replication();
        int ger = req.tuple(i).global_ebs_replication();
        cout << "Received a key request for key " + key + ".\n";

        // fill in placement metadata only if not already exist
        if (placement.find(key) == placement.end()) {
          placement[key] = key_info(gmr, ger);
        }

        // check if the node is responsible for this key
        auto result = responsible<master_node_t, crc32_hasher>(key, placement[key].global_memory_replication_, global_memory_hash_ring, mnode.id_);

        if (result.first) {
          communication::Key_Response_Tuple* tp = res.add_tuple();
          tp->set_key(key);
          // for now, randomly select a memory thread
          int tid = 1 + rand()%MEMORY_THREAD_NUM;
          communication::Key_Response_Address* tp_addr = tp->add_address();
          if (sender == "server") {
            tp_addr->set_addr(worker_node_t(ip, tid).id_);
          } else {
            tp_addr->set_addr(worker_node_t(ip, tid).proxy_connection_connect_addr_);
          }
        }
      }

      string response;
      res.SerializeToString(&response);
      zmq_util::send_string(response, &key_address_responder);
    }

    if (pollitems[4].revents & ZMQ_POLLIN) {
      cout << "received changeset address request from the worker threads (for gossiping)\n";
      zmq::message_t msg;
      zmq_util::recv_msg(&changeset_address_responder, msg);
      changeset_data* data = *(changeset_data **)(msg.data());
      changeset_address* res = new changeset_address();
      unordered_map<master_node_t, unordered_set<string>, node_hash> node_map;

      // a set of keys to send to proxy to perform cross tier gossip
      unordered_set<string> key_to_proxy;

      for (auto it = data->second.begin(); it != data->second.end(); it++) {
        string key = *it;
        // check the global ring and request worker addresses from other node's master thread
        auto pos = global_memory_hash_ring.find(key);
        for (int i = 0; i < placement[key].global_memory_replication_; i++) {
          if (pos->second.ip_.compare(ip) != 0) {
            node_map[pos->second].insert(key);
          }
          if (++pos == global_memory_hash_ring.end()) {
            pos = global_memory_hash_ring.begin();
          }
        }

        // check if the key has replica on the ebs tier
        if (placement[key].global_ebs_replication_ != 0) {
          key_to_proxy.insert(key);
        }
      }
      for (auto map_iter = node_map.begin(); map_iter != node_map.end(); map_iter++) {
        // get key address
        communication::Key_Response resp = get_key_address<key_info>(map_iter->first.key_exchange_connect_addr_, "", map_iter->second, key_address_requesters, placement);

        // for each key in the response
        for (int i = 0; i < resp.tuple_size(); i++) {
          string key = resp.tuple(i).key();
          // we only have one address for memory tier
          string target_address = resp.tuple(i).address(0).addr();
          (*res)[target_address].insert(key);
        }
      }

      // check if there are key requests to send to proxy
      if (key_to_proxy.size() != 0) {
        // for now, randomly choose a proxy to contact
        string target_proxy_address = proxy_address[rand() % proxy_address.size()] + ":" + to_string(PROXY_GOSSIP_PORT);
        // get key address
        communication::Key_Response resp = get_key_address<key_info>(target_proxy_address, "E", key_to_proxy, key_address_requesters, placement);

        // for each key, add the address of *every* (there can be multiple)
        // worker thread on the other node that should receive this key
        for (int i = 0; i < resp.tuple_size(); i++) {
          for (int j = 0; j < resp.tuple(i).address_size(); j++) {
            (*res)[resp.tuple(i).address(j).addr()].insert(resp.tuple(i).key());
          }
        }
      }

      zmq_util::send_msg((void*)res, &changeset_address_responder);
      delete data;
    }

    if (pollitems[5].revents & ZMQ_POLLIN) {
      cout << "Node is departing.\n";
      global_memory_hash_ring.erase(mnode);
      for (auto it = global_memory_hash_ring.begin(); it != global_memory_hash_ring.end(); it++) {
        zmq_util::send_string(ip, &cache[it->second.node_depart_connect_addr_]);
      }
      // notify proxies
      for (auto it = proxy_address.begin(); it != proxy_address.end(); it++) {
        zmq_util::send_string("depart:" + ip, &cache[master_node_t(*it, "M").proxy_notify_connect_addr_]);
      }
      // form the key_request map
      unordered_map<string, communication::Key_Request> key_request_map;
      for (auto it = placement.begin(); it != placement.end(); it++) {
        string key = it->first;
        auto pos = global_memory_hash_ring.find(key);
        for (int i = 0; i < placement[key].global_memory_replication_; i++) {
          key_request_map[pos->second.key_exchange_connect_addr_].set_sender("server");
          communication::Key_Request_Tuple* tp = key_request_map[pos->second.key_exchange_connect_addr_].add_tuple();
          tp->set_key(key);
          tp->set_global_memory_replication(placement[key].global_memory_replication_);
          tp->set_global_ebs_replication(placement[key].global_ebs_replication_);

          if (++pos == global_memory_hash_ring.end()) {
            pos = global_memory_hash_ring.begin();
          }
        }
      }
      // send key addrss requests to other server nodes
      // instruct a random worker to send gossip to the new server! (2 phase)
      int tid = 1 + rand()%MEMORY_THREAD_NUM;
      string worker_address = worker_node_t(ip, tid).local_redistribute_addr_;
      redistribution_address* r_address = new redistribution_address();
      for (auto it = key_request_map.begin(); it != key_request_map.end(); it++) {
        string key_req;
        it->second.SerializeToString(&key_req);
        zmq_util::send_string(key_req, &key_address_requesters[it->first]);
        string key_res = zmq_util::recv_string(&key_address_requesters[it->first]);

        communication::Key_Response resp;
        resp.ParseFromString(key_res);

        // for each key in the response
        for (int i = 0; i < resp.tuple_size(); i++) {
          string key = resp.tuple(i).key();
          // we only have one address for memory tier
          string target_address = resp.tuple(i).address(0).addr();
          (*r_address)[target_address].insert(pair<string, bool>(key, true));
        }
      }
      zmq_util::send_msg((void*)r_address, &cache[worker_address]);
      // TODO: once we break here, I don't think that the threads will have
      // finished. they will still be looping.
      break;
    }

    if (pollitems[6].revents & ZMQ_POLLIN) {
      cout << "Received replication factor change request\n";

      // choose a random worker to remove the keys
      int tid = 1 + rand()%MEMORY_THREAD_NUM;
      string worker_address = worker_node_t(ip, tid).local_redistribute_addr_;

      string placement_req = zmq_util::recv_string(&replication_factor_change_puller);
      communication::Placement_Request req;
      req.ParseFromString(placement_req);

      unordered_map<master_node_t, unordered_set<string>, node_hash> node_map;
      // a set of keys to send to proxy to perform cross tier gossip
      unordered_set<string> key_to_proxy;
      // keep track of which key should be removed
      unordered_map<string, bool> key_remove_map;

      // for every key, update the replication factor and 
      // check if the node is still responsible for the key
      for (int i = 0; i < req.tuple_size(); i++) {
        string key = req.tuple(i).key();

        int gmr = req.tuple(i).global_memory_replication();
        int ger = req.tuple(i).global_ebs_replication();

        // fill in placement metadata only if not already exist
        if (placement.find(key) == placement.end()) {
          placement[key] = key_info(gmr, ger);
        }

        // first, check whether the node is responsible for the key before the replication factor change
        auto result = responsible<master_node_t, crc32_hasher>(key, placement[key].global_memory_replication_, global_memory_hash_ring, mnode.id_);
        // update placement info
        placement[key].global_memory_replication_ = gmr;
        placement[key].global_ebs_replication_ = ger;
        // proceed only if the node is responsible for the key before the replication factor change
        if (result.first) {
          // check the global ring and request worker addresses from other node's master thread
          auto pos = global_memory_hash_ring.find(key);
          for (int i = 0; i < placement[key].global_memory_replication_; i++) {
            if (pos->second.ip_.compare(ip) != 0) {
              node_map[pos->second].insert(key);
            }
            if (++pos == global_memory_hash_ring.end()) {
              pos = global_memory_hash_ring.begin();
            }
          }

          // check if the key has replica on the ebs tier
          if (placement[key].global_ebs_replication_ != 0) {
            key_to_proxy.insert(key);
          }

          // check if the key has to be removed under the new replication factor
          result = responsible<master_node_t, crc32_hasher>(key, placement[key].global_memory_replication_, global_memory_hash_ring, mnode.id_);
          if (result.first) {
            key_remove_map[key] = false;
          } else {
            key_remove_map[key] = true;
          }
        }
      }

      redistribution_address* r_address = new redistribution_address();

      for (auto map_iter = node_map.begin(); map_iter != node_map.end(); map_iter++) {
        // get key address
        communication::Key_Response resp = get_key_address<key_info>(map_iter->first.key_exchange_connect_addr_, "", map_iter->second, key_address_requesters, placement);

        // for each key in the response
        for (int i = 0; i < resp.tuple_size(); i++) {
          string key = resp.tuple(i).key();
          // we only have one address for memory tier
          string target_address = resp.tuple(i).address(0).addr();
          (*r_address)[target_address].insert(pair<string, bool>(key, key_remove_map[key]));
        }
      }

      // check if there are key requests to send to proxy
      if (key_to_proxy.size() != 0) {
        // for now, randomly choose a proxy to contact
        string target_proxy_address = proxy_address[rand() % proxy_address.size()] + ":" + to_string(PROXY_GOSSIP_PORT);
        // get key address
        communication::Key_Response resp = get_key_address<key_info>(target_proxy_address, "E", key_to_proxy, key_address_requesters, placement);

        // for each key, add the address of *every* (there can be multiple)
        // worker thread on the other node that should receive this key
        for (int i = 0; i < resp.tuple_size(); i++) {
          for (int j = 0; j < resp.tuple(i).address_size(); j++) {
            (*r_address)[resp.tuple(i).address(j).addr()].insert(pair<string, bool>(resp.tuple(i).key(), key_remove_map[resp.tuple(i).key()]));
          }
        }
      }

      zmq_util::send_msg((void*)r_address, &cache[worker_address]);
    }

    end = std::chrono::system_clock::now();
    if (chrono::duration_cast<std::chrono::seconds>(end-start).count() >= STORAGE_CONSUMPTION_REPORT_THRESHOLD) {
      // compute total storage consumption
      size_t consumption = 0;
      for (auto it = kvs->size_map.begin(); it != kvs->size_map.end(); it++) {
        consumption += it->second->load();
      }

      string target_proxy_address = "tcp://" + proxy_address[rand() % proxy_address.size()] + ":" + to_string(PROXY_STORAGE_CONSUMPTION_PORT);
      communication::Storage_Update su;
      su.set_node_ip(mnode.ip_);
      su.set_node_type("M");
      su.set_memory_storage(consumption);
      string msg;
      su.SerializeToString(&msg);

      // send the storage consumption update
      zmq_util::send_string(msg, &cache[target_proxy_address]);

      start = std::chrono::system_clock::now();
    }
  }
  for (auto& th: memory_threads) {
    th.join();
  }

  return 0;
}
