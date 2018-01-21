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

// If the total number of updates to the kvs before the last gossip reaches THRESHOLD, then the thread gossips to others.
#define THRESHOLD 1000

// Define the gossip period (frequency)
#define PERIOD 10

// Define node type
#define NODE_TYPE "E"

// Define the locatioon of the conf file with the ebs root path
#define EBS_ROOT_FILE "conf/server/ebs_root.txt"

// TODO: reconsider type names here
typedef KV_Store<string, RC_KVS_PairLattice<string>> Database;

typedef consistent_hash_map<worker_node_t,local_hasher> local_hash_t;

// an unordered map to represent the gossip we are sending
typedef unordered_map<string, RC_KVS_PairLattice<string>> gossip_data;

// a pair to keep track of where each key in the changeset should be sent
typedef pair<size_t, unordered_set<string>> changeset_data;

typedef pair<string, size_t> storage_data;

// a map that represents which keys should be sent to which IP-port
// combinations
typedef unordered_map<string, unordered_set<string>> changeset_address;

// similar to the above but also tells each worker node whether or not it
// should delete the key
typedef unordered_map<string, unordered_set<pair<string, bool>, pair_hash>> redistribution_address;

// TODO:  this should be changed to something more globally robust
atomic<int> lww_timestamp(0);

atomic<int> worker_thread_status[EBS_THREAD_NUM] = {};

string ebs_root("empty");

string get_ebs_path(string subpath) {
  if (ebs_root == "empty") {
    ifstream address;

    address.open(EBS_ROOT_FILE);
    std::getline(address, ebs_root);
    address.close();

    if (ebs_root.back() != '/') {
      ebs_root = ebs_root + "/";
    }
  }

  return ebs_root + subpath;
}

// TODO: more intelligent error mesages throughout
pair<RC_KVS_PairLattice<string>, bool> process_get(string key, int thread_id) {
  RC_KVS_PairLattice<string> res;
  bool succeed = true;

  communication::Payload pl;
  string fname = get_ebs_path("ebs_" + to_string(thread_id) + "/" + key);

  // open a new filestream for reading in a binary
  fstream input(fname, ios::in | ios::binary);

  if (!input) {
    succeed = false;
  } else if (!pl.ParseFromIstream(&input)) {
    cerr << "Failed to parse payload." << endl;
    succeed = false;
  } else {
    res = RC_KVS_PairLattice<string>(timestamp_value_pair<string>(pl.timestamp(), pl.value()));
  }

  // we return a lattice here because this method is invoked for gossip in
  // addition to user requests
  return pair<RC_KVS_PairLattice<string>, bool>(res, succeed);
}

bool process_put(string key, int timestamp, string value, int thread_id, unordered_set<string>& key_set, unordered_map<string, size_t>& size_map) {
  bool succeed = true;
  timestamp_value_pair<string> p = timestamp_value_pair<string>(timestamp, value);

  communication::Payload pl_orig;
  communication::Payload pl;

  string fname = get_ebs_path("ebs_" + to_string(thread_id) + "/" + key);
  fstream input(fname, ios::in | ios::binary);

  if (!input) { // in this case, this key has never been seen before, so we attempt to create a new file for it
    // update value size
    size_map[key] = value.length();
    pl.set_timestamp(timestamp);
    pl.set_value(value);

    // ios::trunc means that we overwrite the existing file
    fstream output(fname, ios::out | ios::trunc | ios::binary);
    if (!pl.SerializeToOstream(&output)) {
      cerr << "Failed to write payload." << endl;
      succeed = false;
    }
  } else if (!pl_orig.ParseFromIstream(&input)) { // if we have seen the key before, attempt to parse what was there before
    cerr << "Failed to parse payload." << endl;
    succeed = false;
  } else {
    // get the existing value that we have and merge
    RC_KVS_PairLattice<string> l = RC_KVS_PairLattice<string>(timestamp_value_pair<string>(pl_orig.timestamp(), pl_orig.value()));
    bool replaced = l.Merge(p);
    if (replaced) {
      // update value size
      size_map[key] = value.length();

      // set the payload's data to the merged values of the value and timestamp
      pl.set_timestamp(l.reveal().timestamp);
      pl.set_value(l.reveal().value);

      // write out the new payload.
      fstream output(fname, ios::out | ios::trunc | ios::binary);
      if (!pl.SerializeToOstream(&output)) {
        cerr << "Failed to write payload\n";
        succeed = false;
      }
    }
  }

  if (succeed) {
    key_set.insert(key);
  }

  return succeed;
}

string process_proxy_request(communication::Request& req, int thread_id, unordered_set<string>& local_changeset, unordered_set<string>& key_set, unordered_map<string, size_t>& size_map) {
  communication::Response response;

  if (req.type() == "GET") {
    cout << "received get by thread " << thread_id << "\n";
    response.set_type("GET");
    for (int i = 0; i < req.tuple_size(); i++) {
      auto res = process_get(req.tuple(i).key(), thread_id);
      communication::Response_Tuple* tp = response.add_tuple();
      tp->set_key(req.tuple(i).key());
      tp->set_value(res.first.reveal().value);
      tp->set_timestamp(res.first.reveal().timestamp);
      tp->set_succeed(res.second);
    }
  } else if (req.type() == "PUT") {
    cout << "received put by thread " << thread_id << "\n";
    response.set_type("PUT");
    for (int i = 0; i < req.tuple_size(); i++) {
      bool succeed = process_put(req.tuple(i).key(), lww_timestamp.load(), req.tuple(i).value(), thread_id, key_set, size_map);
      communication::Response_Tuple* tp = response.add_tuple();
      tp->set_key(req.tuple(i).key());
      tp->set_succeed(succeed);
      local_changeset.insert(req.tuple(i).key());
    }
  }

  string data;
  response.SerializeToString(&data);
  return data;
}

void process_distributed_gossip(communication::Gossip& gossip, int thread_id, unordered_set<string>& key_set, unordered_map<string, size_t>& size_map) {
  for (int i = 0; i < gossip.tuple_size(); i++) {
    process_put(gossip.tuple(i).key(), gossip.tuple(i).timestamp(), gossip.tuple(i).value(), thread_id, key_set, size_map);
  }
}

// This is not serialized into protobuf and using the method above for serializiation overhead
void process_local_gossip(gossip_data* g_data, int thread_id, unordered_set<string>& key_set, unordered_map<string, size_t>& size_map) {
  for (auto it = g_data->begin(); it != g_data->end(); it++) {
    process_put(it->first, it->second.reveal().timestamp, it->second.reveal().value, thread_id, key_set, size_map);
  }
  delete g_data;
}

void send_gossip(changeset_address* change_set_addr, SocketCache& pushers, string ip, int thread_id) {
  unordered_map<string, gossip_data*> local_gossip_map;
  unordered_map<string, communication::Gossip> distributed_gossip_map;

  for (auto map_it = change_set_addr->begin(); map_it != change_set_addr->end(); map_it++) {
    vector<string> v;
    split(map_it->first, ':', v);
    worker_node_t wnode = worker_node_t(v[0], stoi(v[1]) - SERVER_PORT);

    if (v[0] == ip) { // add to local gossip map
      local_gossip_map[wnode.local_gossip_addr_] = new gossip_data;

      // iterate over all of the gossip going to this destination
      for (auto set_it = map_it->second.begin(); set_it != map_it->second.end(); set_it++) {
        auto res = process_get(*set_it, thread_id);

        if (res.second) {
          cout << "Local gossip key " + *set_it + " sent on thread " + to_string(thread_id) + ".\n";
          local_gossip_map[wnode.local_gossip_addr_]->emplace(*set_it, res.first);
        }
      }
    } else { // add to distributed gossip map
      string gossip_addr = wnode.distributed_gossip_connect_addr_;
      for (auto set_it = map_it->second.begin(); set_it != map_it->second.end(); set_it++) {
        auto res = process_get(*set_it, thread_id);

        if (res.second) {
          cout << "Distributed gossip key " + *set_it + " sent on thread " + to_string(thread_id) + ".\n";
          communication::Gossip_Tuple* tp = distributed_gossip_map[gossip_addr].add_tuple();
          tp->set_key(*set_it);
          tp->set_value(res.first.reveal().value);
          tp->set_timestamp(res.first.reveal().timestamp);
        }
      }
    }
  }

  // send local gossip
  for (auto it = local_gossip_map.begin(); it != local_gossip_map.end(); it++) {
    zmq_util::send_msg((void*)it->second, &pushers[it->first]);
  }

  // send distributed gossip
  for (auto it = distributed_gossip_map.begin(); it != distributed_gossip_map.end(); it++) {
    string data;
    it->second.SerializeToString(&data);
    zmq_util::send_string(data, &pushers[it->first]);
  }
}

// worker event loop
void worker_routine (zmq::context_t* context, string ip, int thread_id) {
  size_t port = SERVER_PORT + thread_id;

  // the set of all the keys that this particular thread is responsible for
  unordered_set<string> key_set;

  // the set of changes made on this thread since the last round of gossip
  unordered_set<string> local_changeset;
  worker_node_t wnode = worker_node_t(ip, thread_id);

  // keep track of the size of values
  unordered_map<string, size_t> size_map;

  // socket that respond to proxy requests
  zmq::socket_t responder(*context, ZMQ_REP);
  responder.bind(wnode.proxy_connection_bind_addr_);

  // socket that listens for distributed gossip
  zmq::socket_t dgossip_puller(*context, ZMQ_PULL);
  dgossip_puller.bind(wnode.distributed_gossip_bind_addr_);

  // socket that listens for local gossip
  zmq::socket_t lgossip_puller(*context, ZMQ_PULL);
  lgossip_puller.bind(wnode.local_gossip_addr_);

  // socket that listens for local gossip
  zmq::socket_t lredistribute_puller(*context, ZMQ_PULL);
  lredistribute_puller.bind(wnode.local_redistribute_addr_);

  // used to communicate with master thread for changeset addresses
  zmq::socket_t changeset_address_requester(*context, ZMQ_REQ);
  changeset_address_requester.connect(CHANGESET_ADDR);

  // used to send gossip
  SocketCache pushers(context, ZMQ_PUSH);

  //  Initialize poll set
  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(responder), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(dgossip_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(lgossip_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(lredistribute_puller), 0, ZMQ_POLLIN, 0 }
  };

  auto gossip_start = std::chrono::system_clock::now();
  auto gossip_end = std::chrono::system_clock::now();
  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  // Enter the event loop
  while (true) {
    zmq_util::poll(0, &pollitems);

    if (pollitems[0].revents & ZMQ_POLLIN) { // process a request from the proxy
      worker_thread_status[thread_id - 1].store(1);
      lww_timestamp++;
      string data = zmq_util::recv_string(&responder);
      communication::Request req;
      req.ParseFromString(data);

      //  Process request
      string result = process_proxy_request(req, thread_id, local_changeset, key_set, size_map);

      //  Send reply back to proxy
      zmq_util::send_string(result, &responder);
      worker_thread_status[thread_id - 1].store(0);
    }

    if (pollitems[1].revents & ZMQ_POLLIN) { // process distributed gossip
      worker_thread_status[thread_id - 1].store(1);
      cout << "Received distributed gossip on thread " + to_string(thread_id) + ".\n";

      string data = zmq_util::recv_string(&dgossip_puller);
      communication::Gossip gossip;
      gossip.ParseFromString(data);

      //  Process distributed gossip
      process_distributed_gossip(gossip, thread_id, key_set, size_map);
      worker_thread_status[thread_id - 1].store(0);
    }

    if (pollitems[2].revents & ZMQ_POLLIN) { // process local gossip
      worker_thread_status[thread_id - 1].store(1);
      cout << "Received local gossip on thread " + to_string(thread_id) + ".\n";

      zmq::message_t msg;
      zmq_util::recv_msg(&lgossip_puller, msg);
      gossip_data* g_data = *(gossip_data **)(msg.data());

      //  Process local gossip
      process_local_gossip(g_data, thread_id, key_set, size_map);
      worker_thread_status[thread_id - 1].store(0);
    }

    if (pollitems[3].revents & ZMQ_POLLIN) { // process a local redistribute request
      worker_thread_status[thread_id - 1].store(1);
      cout << "Received local redistribute request on thread " + to_string(thread_id) + ".\n";

      zmq::message_t msg;
      zmq_util::recv_msg(&lredistribute_puller, msg);

      redistribution_address* r_data = *(redistribution_address **)(msg.data());
      changeset_address c_address;
      unordered_set<string> remove_set;

      // converting the redistribution_address to a changeset_address
      for (auto map_it = r_data->begin(); map_it != r_data->end(); map_it++) {
        for (auto set_it = map_it->second.begin(); set_it != map_it->second.end(); set_it++) {
          c_address[map_it->first].insert(set_it->first);
          if (set_it->second) {
            remove_set.insert(set_it->first);
          }
        }
      }

      send_gossip(&c_address, pushers, ip, thread_id);
      delete r_data;

      // remove keys in the remove set
      for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
        key_set.erase(*it);
        // update value size
        size_map.erase(*it);

        string fname = get_ebs_path("ebs_" + to_string(thread_id) + "/" + *it);
        if(remove(fname.c_str()) != 0) {
          cout << "Error deleting file";
        } else {
          cout << "File successfully deleted";
        }
      }
      worker_thread_status[thread_id - 1].store(0);
    }

    gossip_end = std::chrono::system_clock::now();
    // check whether we should gossip
    if (chrono::duration_cast<std::chrono::seconds>(gossip_end-gossip_start).count() >= PERIOD || local_changeset.size() >= THRESHOLD) {
      // only gossip if we have changes
      if (local_changeset.size() > 0) {
        // populate the data that has changed
        changeset_data* data = new changeset_data();
        data->first = port;

        for (auto it = local_changeset.begin(); it != local_changeset.end(); it++) {
          (data->second).insert(*it);
        }

        // send a message to the master thread requesting the addresses of the
        // desinations for gossip
        zmq_util::send_msg((void*)data, &changeset_address_requester);
        zmq::message_t msg;
        zmq_util::recv_msg(&changeset_address_requester, msg);

        // send the gossip
        changeset_address* res = *(changeset_address **)(msg.data());
        send_gossip(res, pushers, ip, thread_id);
        delete res;
        local_changeset.clear();
      }

      gossip_start = std::chrono::system_clock::now();
    }

    report_end = std::chrono::system_clock::now();
    // check whether we should send storage consumption update
    if (chrono::duration_cast<std::chrono::seconds>(report_end-report_start).count() >= SERVER_REPORT_THRESHOLD) {
      storage_data* data = new storage_data();

      data->first = to_string(thread_id);

      // compute total storage consumption
      size_t consumption = 0;
      for (auto it = size_map.begin(); it != size_map.end(); it++) {
        consumption += it->second;
      }
      data->second = consumption;

      zmq_util::send_msg((void*)data, &pushers[LOCAL_STORAGE_CONSUMPTION_ADDR]);

      report_start = std::chrono::system_clock::now();
    }
  }
}

int main(int argc, char* argv[]) {

  auto logger = spdlog::basic_logger_mt("basic_logger", "log.txt", true);
  logger->flush_on(spdlog::level::info); 
  
  if (argc != 2) {
    cerr << "usage:" << argv[0] << " <new_node>" << endl;
    return 1;
  }

  if (string(argv[1]) != "y" && string(argv[1]) != "n") {
    cerr << "Invalid first argument: " << string(argv[1]) << "." << endl;
    return 1;
  }

  string ip = get_ip("server");
  string new_node = argv[1];

  master_node_t mnode = master_node_t(ip, NODE_TYPE);

  // prepare the zmq context
  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);
  SocketCache requesters(&context, ZMQ_REQ);

  // create our hash rings
  global_hash_t global_hash_ring;
  local_hash_t local_hash_ring;
  unordered_map<string, storage_key_info> placement;

  // keep track of the storage consumption for all worker threads
  unordered_map<string, size_t> worker_thread_storage;

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

  // use the first address for now
  monitoring_node_t monitoring_node = monitoring_node_t(*monitoring_address.begin());

  cerr << "getting info from seed\n";

  // read server address from the file
  if (new_node == "n") {
    address.open("conf/server/start_servers.txt");
    // add itself to the ring
    global_hash_ring.insert(mnode);

    // add all other servers
    while (getline(address, ip_line)) {
      global_hash_ring.insert(master_node_t(ip_line, NODE_TYPE));
    }
    address.close();
  } else { // get server address from the seed node
    address.open("conf/server/seed_server.txt");
    getline(address, ip_line);
    address.close();

    cerr << "seed address is " + ip_line + "\n";

    // tell the seed node that you are joining
    zmq::socket_t addr_requester(context, ZMQ_REQ);
    addr_requester.connect(master_node_t(ip_line, NODE_TYPE).seed_connection_connect_addr_);
    zmq_util::send_string("join", &addr_requester);

    // receive and add all the addresses that seed node sent
    vector<string> addresses;
    split(zmq_util::recv_string(&addr_requester), '|', addresses);
    for (auto it = addresses.begin(); it != addresses.end(); it++) {
      global_hash_ring.insert(master_node_t(*it, NODE_TYPE));
    }

    // add itself to global hash ring
    global_hash_ring.insert(mnode);
  }

  cerr << "finished getting info from seed\n";

  vector<thread> worker_threads;

  // start the initial threads based on EBS_THREAD_NUM
  for (int thread_id = 1; thread_id <= EBS_THREAD_NUM; thread_id++) {
    //cout << "thread id is " + to_string(thread_id) + "\n";
    worker_threads.push_back(thread(worker_routine, &context, ip, thread_id));
    local_hash_ring.insert(worker_node_t(ip, thread_id));
  }

  cerr << "notifying other server nodes\n";

  if (new_node == "y") {
    // notify other servers that there is a new node
    for (auto it = global_hash_ring.begin(); it != global_hash_ring.end(); it++) {
      if (it->second.ip_.compare(ip) != 0) {
        zmq_util::send_string(ip, &pushers[(it->second).node_join_connect_addr_]);
      }
    }
  }

  cerr << "finished notifying other server nodes\n";
  cerr << "notifying proxy\n";

  string msg = "join:" + string(NODE_TYPE) + ":" + ip;
  // notify proxies that this node has joined the service
  for (auto it = proxy_address.begin(); it != proxy_address.end(); it++) {
    zmq_util::send_string(msg, &pushers[proxy_node_t(*it).notify_connect_addr_]);
  }
  cerr << "notifying monitoring\n";

  // notify monitoring nodes that this node has joined the service
  for (auto it = monitoring_address.begin(); it != monitoring_address.end(); it++) {
    zmq_util::send_string(msg, &pushers[monitoring_node_t(*it).notify_connect_addr_]);
  }
  cerr << "finished notifying monitoring\n";

  // responsible for sending the server address to a new node
  zmq::socket_t addr_responder(context, ZMQ_REP);
  addr_responder.bind(SEED_BIND_ADDR);

  // listens for a new node joining
  zmq::socket_t join_puller(context, ZMQ_PULL);
  join_puller.bind(NODE_JOIN_BIND_ADDR);

  // listens for a node departing
  zmq::socket_t depart_puller(context, ZMQ_PULL);
  depart_puller.bind(NODE_DEPART_BIND_ADDR);

  // responsible for sending the worker address (responsible for the requested key) to the proxy or other to servers
  zmq::socket_t key_address_responder(context, ZMQ_REP);
  key_address_responder.bind(KEY_EXCHANGE_BIND_ADDR);

  // responsible for responding to changeset address requests from workers
  zmq::socket_t changeset_address_responder(context, ZMQ_REP);
  changeset_address_responder.bind(CHANGESET_ADDR);

  // responsible for listening for a command that this node should leave
  zmq::socket_t self_depart_responder(context, ZMQ_REP);
  self_depart_responder.bind(SELF_DEPART_BIND_ADDR);

  // responsible for listening for key replication factor change
  zmq::socket_t replication_factor_change_puller(context, ZMQ_PULL);
  replication_factor_change_puller.bind(REPLICATION_FACTOR_BIND_ADDR);

  // responsible for receiving storage consumption updates from worker threads
  zmq::socket_t storage_consumption_puller(context, ZMQ_PULL);
  storage_consumption_puller.bind(LOCAL_STORAGE_CONSUMPTION_ADDR);

  // set up zmq receivers
  vector<zmq::pollitem_t> pollitems = {{static_cast<void*>(addr_responder), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(join_puller), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(depart_puller), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(key_address_responder), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(changeset_address_responder), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(self_depart_responder), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(replication_factor_change_puller), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(storage_consumption_puller), 0, ZMQ_POLLIN, 0}
                                     };

  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  cerr << "entering event loop\n";

  // enter event loop
  while (true) {
    zmq_util::poll(0, &pollitems);

    if (pollitems[0].revents & ZMQ_POLLIN) {
      logger->info("Received an address request");
      string request = zmq_util::recv_string(&addr_responder);

      string addresses;
      for (auto it = global_hash_ring.begin(); it != global_hash_ring.end(); it++) {
        addresses += (it->second.ip_ + "|");
      }

      // remove the trailing pipe
      addresses.pop_back();
      zmq_util::send_string(addresses, &addr_responder);
    }

    if (pollitems[1].revents & ZMQ_POLLIN) {
      string new_server_ip = zmq_util::recv_string(&join_puller);
      master_node_t new_node = master_node_t(new_server_ip, NODE_TYPE);

      // update global hash ring
      bool inserted = global_hash_ring.insert(new_node).second;

      if (inserted) {
        logger->info("Received a node join. New node is {}", new_server_ip);
        for (auto it = global_hash_ring.begin(); it != global_hash_ring.end(); it++) {
          if (it->second.ip_.compare(ip) != 0 && it->second.ip_.compare(new_server_ip) != 0) {
            // if the node is not myself and not the newly joined node, send the ip of the newly joined node
            zmq_util::send_string(new_server_ip, &pushers[(it->second).node_join_connect_addr_]);
          } else if (it->second.ip_.compare(new_server_ip) == 0) {
            // if the node is the newly joined node, send my ip
            zmq_util::send_string(ip, &pushers[(it->second).node_join_connect_addr_]);
          }
        }
        // a map for each local worker that has to redistribute to the remote
        // node
        unordered_map<string, redistribution_address*> redistribution_map;

        // for each key in this set, we will ask the new node which worker the
        // key should be sent to
        unordered_set<string> key_to_query;

        // iterate through all of my keys
        for (auto it = placement.begin(); it != placement.end(); it++) {

          string key = it->first;
          storage_key_info info = it->second;

          auto result = responsible<master_node_t, global_hasher>(key, info.global_ebs_replication_, global_hash_ring, new_node.id_);

          if (result.first && (result.second->ip_.compare(ip) == 0)) {
            key_to_query.insert(it->first);
          }
        }

        communication::Key_Response resp = get_key_address<storage_key_info>(new_node.key_exchange_connect_addr_, "", key_to_query, requesters, placement);

        // for each key in the response
        for (int i = 0; i < resp.tuple_size(); i++) {
          // for each of the workers we are sending the key to (if the local
          // replication factor is > 1)
          for (int j = 0; j < resp.tuple(i).address_size(); j++) {
            string key = resp.tuple(i).key();
            string target_address = resp.tuple(i).address(j).addr();

            // TODO; only send from *one* local replica not from many...
            // figure out how to tell local replica to *remove* but not gossip
            // for each replica of the key on this machine
            auto pos = local_hash_ring.find(key);
            for (int k = 0; k < placement[key].local_replication_; k++) {
              // create a redistribution_address object for each of the local
              // workers
              string worker_address = pos->second.local_redistribute_addr_;
              if (redistribution_map.find(worker_address) == redistribution_map.end()) {
                redistribution_map[worker_address] = new redistribution_address();
              }

              // insert this key into the redistrution address for each of the
              // local workers based on the destination
              (*redistribution_map[worker_address])[target_address].insert(pair<string, bool>(key, true));

              // go to the next position on the ring
              if (++pos == local_hash_ring.end()) {
                pos = local_hash_ring.begin();
              }
            }
          }
        }

        // send a local message for each local worker that has to redistribute
        // data to a remote node
        for (auto it = redistribution_map.begin(); it != redistribution_map.end(); it++) {
          zmq_util::send_msg((void*)it->second, &pushers[it->first]);
        }
      }
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      string departing_server_ip = zmq_util::recv_string(&depart_puller);
      logger->info("Received departure for node {}", departing_server_ip);

      // update hash ring
      global_hash_ring.erase(master_node_t(departing_server_ip, NODE_TYPE));
    }

    if (pollitems[3].revents & ZMQ_POLLIN) {
      //cout << "Received a key address request.\n";

      string serialized_key_req = zmq_util::recv_string(&key_address_responder);
      communication::Key_Request key_req;
      key_req.ParseFromString(serialized_key_req);

      string sender = key_req.sender();
      communication::Key_Response key_resp;

      // for every requested key
      for (int i = 0; i < key_req.tuple_size(); i++) {
        string key = key_req.tuple(i).key();
        int gmr = key_req.tuple(i).global_memory_replication();
        int ger = key_req.tuple(i).global_ebs_replication();
        //cout << "Received a key request for key " + key + ".\n";

        // fill in placement metadata only if not already exist
        if (placement.find(key) == placement.end()) {
          placement[key] = storage_key_info(gmr, ger);
        }

        // check if the node is responsible for this key
        auto result = responsible<master_node_t, global_hasher>(key, placement[key].global_ebs_replication_, global_hash_ring, mnode.id_);

        if (result.first) {
          communication::Key_Response_Tuple* tp = key_resp.add_tuple();
          tp->set_key(key);

          // find all the local worker threads that are assigned to this key
          auto it = local_hash_ring.find(key);
          for (int i = 0; i < placement[key].local_replication_; i++) {
            // add each one to the response
            communication::Key_Response_Address* tp_addr = tp->add_address();
            if (sender == "server") {
              tp_addr->set_addr(it->second.id_);
            } else {
              tp_addr->set_addr(it->second.proxy_connection_connect_addr_);
            }

            if (++it == local_hash_ring.end()) {
              it = local_hash_ring.begin();
            }
          }
        }
      }

      string serialized_key_resp;
      key_resp.SerializeToString(&serialized_key_resp);
      zmq_util::send_string(serialized_key_resp, &key_address_responder);
    }

    if (pollitems[4].revents & ZMQ_POLLIN) {
      //cout << "Received a changeset address request from a worker thread.\n";

      zmq::message_t msg;
      zmq_util::recv_msg(&changeset_address_responder, msg);
      changeset_data* data = *(changeset_data **)(msg.data());

      // determine the IP and port of the thread that made the request
      string self_id = ip + ":" + to_string(data->first);
      changeset_address* res = new changeset_address();
      unordered_map<master_node_t, unordered_set<string>, node_hash> node_map;

      // a set of keys to send to proxy to perform cross tier gossip
      unordered_set<string> key_to_proxy;

      // look for every key requestsed by the worker thread
      for (auto it = data->second.begin(); it != data->second.end(); it++) {
        string key = *it;
        // first, check the local ring
        auto local_pos = local_hash_ring.find(key);
        for (int i = 0; i < placement[key].local_replication_; i++) {
          // add any thread that is responsible for this key but is not the
          // thread that made the request
          if (local_pos->second.id_.compare(self_id) != 0) {
            (*res)[local_pos->second.id_].insert(key);
          }

          if (++local_pos == local_hash_ring.end()) {
            local_pos = local_hash_ring.begin();
          }
        }

        // second, check the global ebs ring for any nodes that might also be
        // responsible for this key
        auto pos = global_hash_ring.find(key);
        for (int i = 0; i < placement[key].global_ebs_replication_; i++) {
          if (pos->second.ip_.compare(ip) != 0) {
            node_map[pos->second].insert(key);
          }

          if (++pos == global_hash_ring.end()) {
            pos = global_hash_ring.begin();
          }
        }

        // finally, check if the key has replica on the other tier
        if (placement[key].global_memory_replication_ != 0) {
          key_to_proxy.insert(key);
        }
      }

      // for any remote nodes that should receive gossip, we make key
      // requests
      for (auto map_iter = node_map.begin(); map_iter != node_map.end(); map_iter++) {
        // get key address
        communication::Key_Response resp = get_key_address<storage_key_info>(map_iter->first.key_exchange_connect_addr_, "", map_iter->second, requesters, placement);

        // for each key, add the address of *every* (there can be multiple)
        // worker thread on the other node that should receive this key
        for (int i = 0; i < resp.tuple_size(); i++) {
          for (int j = 0; j < resp.tuple(i).address_size(); j++) {
            (*res)[resp.tuple(i).address(j).addr()].insert(resp.tuple(i).key());
          }
        }
      }

      // check if there are key requests to send to proxy
      if (key_to_proxy.size() != 0) {
        // for now, randomly choose a proxy worker to contact
        int tid = 1 + rand() % PROXY_THREAD_NUM;
        string target_proxy_address = proxy_worker_thread_t(proxy_address[rand() % proxy_address.size()], tid).proxy_gossip_connect_addr_;
        // get key address
        communication::Key_Response resp = get_key_address<storage_key_info>(target_proxy_address, NODE_TYPE, key_to_proxy, requesters, placement);

        // for each key, add the address of *every* (there can be multiple)
        // worker thread on the other node that should receive this key
        for (int i = 0; i < resp.tuple_size(); i++) {
          for (int j = 0; j < resp.tuple(i).address_size(); j++) {
            (*res)[resp.tuple(i).address(j).addr()].insert(resp.tuple(i).key());
          }
        }
      }

      // send the resulting changeset_address object back to the thread
      zmq_util::send_msg((void*)res, &changeset_address_responder);
      delete data;
    }

    if (pollitems[5].revents & ZMQ_POLLIN) {
      logger->info("Node is departing");

      global_hash_ring.erase(mnode);
      for (auto it = global_hash_ring.begin(); it != global_hash_ring.end(); it++) {
        zmq_util::send_string(ip, &pushers[it->second.node_depart_connect_addr_]);
      }
      
      string msg = "depart:" + string(NODE_TYPE) + ":" + ip;
      // notify proxies
      for (auto it = proxy_address.begin(); it != proxy_address.end(); it++) {
        zmq_util::send_string(msg, &pushers[proxy_node_t(*it).notify_connect_addr_]);
      }

      // form the key_request map
      unordered_map<string, communication::Key_Request> key_request_map;
      for (auto it = placement.begin(); it != placement.end(); it++) {
        string key = it->first;
        auto pos = global_hash_ring.find(key);

        for (int i = 0; i < placement[key].global_ebs_replication_; i++) {
          key_request_map[pos->second.key_exchange_connect_addr_].set_sender("server");
          communication::Key_Request_Tuple* tp = key_request_map[pos->second.key_exchange_connect_addr_].add_tuple();
          tp->set_key(key);
          tp->set_global_memory_replication(placement[key].global_memory_replication_);
          tp->set_global_ebs_replication(placement[key].global_ebs_replication_);

          if (++pos == global_hash_ring.end()) {
            pos = global_hash_ring.begin();
          }
        }
      }

      // send key address requests to other server nodes
      unordered_map<string, redistribution_address*> redistribution_map;
      for (auto it = key_request_map.begin(); it != key_request_map.end(); it++) {
        string key_req;
        it->second.SerializeToString(&key_req);
        zmq_util::send_string(key_req, &requesters[it->first]);

        string key_res = zmq_util::recv_string(&requesters[it->first]);
        communication::Key_Response resp;
        resp.ParseFromString(key_res);

        for (int i = 0; i < resp.tuple_size(); i++) {
          for (int j = 0; j < resp.tuple(i).address_size(); j++) {
            string key = resp.tuple(i).key();
            string target_address = resp.tuple(i).address(j).addr();
            auto pos = local_hash_ring.find(key);

            for (int k = 0; k < placement[key].local_replication_; k++) {
              string worker_address = pos->second.local_redistribute_addr_;
              if (redistribution_map.find(worker_address) == redistribution_map.end()) {
                redistribution_map[worker_address] = new redistribution_address();
              }

              (*redistribution_map[worker_address])[target_address].insert(pair<string, bool>(key, false));

              if (++pos == local_hash_ring.end()) {
                pos = local_hash_ring.begin();
              }
            }
          }
        }
      }

      for (auto it = redistribution_map.begin(); it != redistribution_map.end(); it++) {
        zmq_util::send_msg((void*)it->second, &pushers[it->first]);
      }
      
      // TODO: once we break here, I don't think that the threads will have
      // finished. they will still be looping.
      break;
    }

    if (pollitems[6].revents & ZMQ_POLLIN) {
      logger->info("Received replication factor change request");

      string placement_req = zmq_util::recv_string(&replication_factor_change_puller);
      communication::Replication_Factor_Request req;
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
          placement[key] = storage_key_info(gmr, ger);
        }

        // first, check whether the node is responsible for the key before the replication factor change
        auto result = responsible<master_node_t, global_hasher>(key, placement[key].global_ebs_replication_, global_hash_ring, mnode.id_);
        // update placement info
        placement[key].global_memory_replication_ = gmr;
        placement[key].global_ebs_replication_ = ger;
        // proceed only if the node is responsible for the key before the replication factor change
        if (result.first) {
          // check the global ring and request worker addresses from other node's master thread
          auto pos = global_hash_ring.find(key);
          for (int i = 0; i < placement[key].global_ebs_replication_; i++) {
            if (pos->second.ip_.compare(ip) != 0) {
              node_map[pos->second].insert(key);
            }
            if (++pos == global_hash_ring.end()) {
              pos = global_hash_ring.begin();
            }
          }

          // check if the key has replica on the other tier
          if (placement[key].global_memory_replication_ != 0) {
            key_to_proxy.insert(key);
          }

          // check if the key has to be removed under the new replication factor
          result = responsible<master_node_t, global_hasher>(key, placement[key].global_ebs_replication_, global_hash_ring, mnode.id_);
          if (result.first) {
            key_remove_map[key] = false;
          } else {
            key_remove_map[key] = true;
          }
        }
      }

      // initialize the redistribution map (key is the address of the worker threads)
      unordered_map<string, redistribution_address*> redistribution_map;

      for (auto map_iter = node_map.begin(); map_iter != node_map.end(); map_iter++) {
        // get key address
        communication::Key_Response resp = get_key_address<storage_key_info>(map_iter->first.key_exchange_connect_addr_, "", map_iter->second, requesters, placement);
        // for each key in the response
        for (int i = 0; i < resp.tuple_size(); i++) {
          // for each of the workers we are sending the key to (if the local
          // replication factor is > 1)
          for (int j = 0; j < resp.tuple(i).address_size(); j++) {
            string key = resp.tuple(i).key();
            string target_address = resp.tuple(i).address(j).addr();

            // TODO; only send from *one* local replica not from many...
            // figure out how to tell local replica to *remove* but not gossip
            // for each replica of the key on this machine
            auto pos = local_hash_ring.find(key);
            for (int k = 0; k < placement[key].local_replication_; k++) {
              // create a redistribution_address object for each of the local
              // workers
              string worker_address = pos->second.local_redistribute_addr_;
              if (redistribution_map.find(worker_address) == redistribution_map.end()) {
                redistribution_map[worker_address] = new redistribution_address();
              }

              // insert this key into the redistrution address for each of the
              // local workers based on the destination
              (*redistribution_map[worker_address])[target_address].insert(pair<string, bool>(key, key_remove_map[key]));

              // go to the next position on the ring
              if (++pos == local_hash_ring.end()) {
                pos = local_hash_ring.begin();
              }
            }
          }
        }
      }

      // check if there are key requests to send to proxy
      if (key_to_proxy.size() != 0) {
        // for now, randomly choose a proxy worker to contact
        int tid = 1 + rand() % PROXY_THREAD_NUM;
        string target_proxy_address = proxy_worker_thread_t(proxy_address[rand() % proxy_address.size()], tid).proxy_gossip_connect_addr_;
        // get key address
        communication::Key_Response resp = get_key_address<storage_key_info>(target_proxy_address, NODE_TYPE, key_to_proxy, requesters, placement);

        // for each key in the response
        for (int i = 0; i < resp.tuple_size(); i++) {
          // for each of the workers we are sending the key to (if the local
          // replication factor is > 1)
          for (int j = 0; j < resp.tuple(i).address_size(); j++) {
            string key = resp.tuple(i).key();
            string target_address = resp.tuple(i).address(j).addr();

            // TODO; only send from *one* local replica not from many...
            // figure out how to tell local replica to *remove* but not gossip
            // for each replica of the key on this machine
            auto pos = local_hash_ring.find(key);
            for (int k = 0; k < placement[key].local_replication_; k++) {
              // create a redistribution_address object for each of the local
              // workers
              string worker_address = pos->second.local_redistribute_addr_;
              if (redistribution_map.find(worker_address) == redistribution_map.end()) {
                redistribution_map[worker_address] = new redistribution_address();
              }

              // insert this key into the redistrution address for each of the
              // local workers based on the destination
              (*redistribution_map[worker_address])[target_address].insert(pair<string, bool>(key, key_remove_map[key]));

              // go to the next position on the ring
              if (++pos == local_hash_ring.end()) {
                pos = local_hash_ring.begin();
              }
            }
          }
        }
      }

      // send a local message for each local worker that has to redistribute
      // data to a remote node
      for (auto it = redistribution_map.begin(); it != redistribution_map.end(); it++) {
        zmq_util::send_msg((void*)it->second, &pushers[it->first]);
      }
    }

    if (pollitems[7].revents & ZMQ_POLLIN) {
      //cerr << "received storage update from worker thread\n";
      zmq::message_t msg;
      zmq_util::recv_msg(&storage_consumption_puller, msg);
      storage_data* data = *(storage_data **)(msg.data());

      worker_thread_storage[data->first] = data->second;

      delete data;
    }

    report_end = std::chrono::system_clock::now();

    if (chrono::duration_cast<std::chrono::seconds>(report_end-report_start).count() >= SERVER_REPORT_THRESHOLD) {
      communication::Request req;
      // report storage consumption
      req.set_type("PUT");
      req.set_metadata(true);

      communication::Request_Tuple* tp = req.add_tuple();
      tp->set_key(mnode.ip_ + "_" + NODE_TYPE + "_storage");

      string storage_val = "";

      for (auto it = worker_thread_storage.begin(); it != worker_thread_storage.end(); it++) {
        storage_val += (it->first + ":" + to_string(it->second) + "|");
      }
      storage_val.pop_back();

      tp->set_value(storage_val);

      // report worker thread occupancy
      tp = req.add_tuple();
      tp->set_key(mnode.ip_ + "_" + NODE_TYPE + "_occupancy");

      string occupancy_val = "";

      for (int i = 0; i < MEMORY_THREAD_NUM; i++) {
        occupancy_val += (to_string(i+1) + ":" + to_string(worker_thread_status[i].load()) + "|");
      }
      occupancy_val.pop_back();

      tp->set_value(occupancy_val);
      string serialized_req;
      req.SerializeToString(&serialized_req);
      // send the storage consumption update to a random proxy worker
      // just pick the first proxy to contact for now;
      // this should eventually be round-robin / random
      string proxy_ip = *(proxy_address.begin());
      // randomly choose a proxy thread to connect
      int tid = 1 + rand() % PROXY_THREAD_NUM;
      zmq_util::send_string(serialized_req, &pushers[proxy_worker_thread_t(proxy_ip, tid).metadata_connect_addr_]);

      report_start = std::chrono::system_clock::now();
    }
  }
}
