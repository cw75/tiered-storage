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
#define THRESHOLD 100

// Define the gossip period (frequency)
#define PERIOD 5

// Define the number of ebs threads
#define EBS_THREAD_NUM 1

// Define the default local ebs replication factor
#define DEFAULT_LOCAL_EBS_REPLICATION 1

// Define the locatioon of the conf file with the ebs root path
#define EBS_ROOT_FILE "conf/server/ebs_root.txt"

// TODO: reconsider type names here
typedef KV_Store<string, RC_KVS_PairLattice<string>> Database;

typedef consistent_hash_map<worker_node_t,ebs_hasher> ebs_hash_t;

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

bool enable_ebs(false);
string ebs_root("empty");

struct ebs_key_info {
  ebs_key_info() : global_memory_replication_(1), global_ebs_replication_(2), local_ebs_replication_(1) {}
  ebs_key_info(int gmr, int ger, int ler)
    : global_memory_replication_(gmr), global_ebs_replication_(ger), local_ebs_replication_(ler) {}
  ebs_key_info(int gmr, int ger)
    : global_memory_replication_(gmr), global_ebs_replication_(ger), local_ebs_replication_(DEFAULT_LOCAL_EBS_REPLICATION) {}
  int global_memory_replication_;
  int global_ebs_replication_;
  int local_ebs_replication_;
};

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
pair<RC_KVS_PairLattice<string>, bool> process_ebs_get(string key, int thread_id) {
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

bool process_ebs_put(string key, int timestamp, string value, int thread_id, unordered_set<string>& key_set, unordered_map<string, size_t>& size_map) {
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

  if (req.get_size() != 0) {
    cout << "received get by thread " << thread_id << "\n";
    for (int i = 0; i < req.get_size(); i++) {
      auto res = process_ebs_get(req.get(i).key(), thread_id);
      communication::Response_Tuple* tp = response.add_tuple();
      tp->set_key(req.get(i).key());
      tp->set_value(res.first.reveal().value);
      tp->set_timestamp(res.first.reveal().timestamp);
      tp->set_succeed(res.second);
    }
  } else if (req.put_size() != 0) {
    cout << "received put by thread " << thread_id << "\n";
    for (int i = 0; i < req.put_size(); i++) {
      bool succeed = process_ebs_put(req.put(i).key(), lww_timestamp.load(), req.put(i).value(), thread_id, key_set, size_map);
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

void process_distributed_gossip(communication::Gossip& gossip, int thread_id, unordered_set<string>& key_set, unordered_map<string, size_t>& size_map) {
  for (int i = 0; i < gossip.tuple_size(); i++) {
    process_ebs_put(gossip.tuple(i).key(), gossip.tuple(i).timestamp(), gossip.tuple(i).value(), thread_id, key_set, size_map);
  }
}

// This is not serialized into protobuf and using the method above for serializiation overhead
void process_local_gossip(gossip_data* g_data, int thread_id, unordered_set<string>& key_set, unordered_map<string, size_t>& size_map) {
  for (auto it = g_data->begin(); it != g_data->end(); it++) {
    process_ebs_put(it->first, it->second.reveal().timestamp, it->second.reveal().value, thread_id, key_set, size_map);
  }
  delete g_data;
}

void send_gossip(changeset_address* change_set_addr, SocketCache& cache, string ip, int thread_id) {
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
        auto res = process_ebs_get(*set_it, thread_id);

        if (res.second) {
          cout << "Local gossip key " + *set_it + " sent on thread " + to_string(thread_id) + ".\n";
          local_gossip_map[wnode.local_gossip_addr_]->emplace(*set_it, res.first);
        }
      }
    } else { // add to distributed gossip map
      string gossip_addr = wnode.distributed_gossip_connect_addr_;
      for (auto set_it = map_it->second.begin(); set_it != map_it->second.end(); set_it++) {
        auto res = process_ebs_get(*set_it, thread_id);

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
    zmq_util::send_msg((void*)it->second, &cache[it->first]);
  }

  // send distributed gossip
  for (auto it = distributed_gossip_map.begin(); it != distributed_gossip_map.end(); it++) {
    string data;
    it->second.SerializeToString(&data);
    zmq_util::send_string(data, &cache[it->first]);
  }
}

// ebs worker event loop
void ebs_worker_routine (zmq::context_t* context, string ip, int thread_id) {
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

  // socket that listens for departure command
  zmq::socket_t depart_puller(*context, ZMQ_PULL);
  depart_puller.bind(wnode.local_depart_addr_);

  // used to communicate with master thread for changeset addresses
  zmq::socket_t changeset_address_requester(*context, ZMQ_REQ);
  changeset_address_requester.connect(CHANGESET_ADDR);

  // used to send gossip
  SocketCache cache(context, ZMQ_PUSH);

  //  Initialize poll set
  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(responder), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(dgossip_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(lgossip_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(lredistribute_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(depart_puller), 0, ZMQ_POLLIN, 0 }
  };

  auto gossip_start = std::chrono::system_clock::now();
  auto gossip_end = std::chrono::system_clock::now();
  auto storage_start = std::chrono::system_clock::now();
  auto storage_end = std::chrono::system_clock::now();

  // Enter the event loop
  while (true) {
    zmq_util::poll(0, &pollitems);

    if (pollitems[0].revents & ZMQ_POLLIN) { // process a request from the proxy
      string data = zmq_util::recv_string(&responder);
      communication::Request req;
      req.ParseFromString(data);

      //  Process request
      string result = process_proxy_request(req, thread_id, local_changeset, key_set, size_map);

      //  Send reply back to proxy
      zmq_util::send_string(result, &responder);
    } else if (pollitems[1].revents & ZMQ_POLLIN) { // process distributed gossip
      cout << "Received distributed gossip on thread " + to_string(thread_id) + ".\n";

      string data = zmq_util::recv_string(&dgossip_puller);
      communication::Gossip gossip;
      gossip.ParseFromString(data);

      //  Process distributed gossip
      process_distributed_gossip(gossip, thread_id, key_set, size_map);
    } else if (pollitems[2].revents & ZMQ_POLLIN) { // process local gossip
      cout << "Received local gossip on thread " + to_string(thread_id) + ".\n";

      zmq::message_t msg;
      zmq_util::recv_msg(&lgossip_puller, msg);
      gossip_data* g_data = *(gossip_data **)(msg.data());

      //  Process local gossip
      process_local_gossip(g_data, thread_id, key_set, size_map);
    } else if (pollitems[3].revents & ZMQ_POLLIN) { // process a local redistribute request
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

      send_gossip(&c_address, cache, ip, thread_id);
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
    } else if (pollitems[4].revents & ZMQ_POLLIN) { // process a departure 
      cout << "Thread " + to_string(thread_id) + " received departure command.\n";

      string device_name = zmq_util::recv_string(&depart_puller);

      changeset_data* data = new changeset_data();
      data->first = port;

      // for every key this thread is responsible for, add it to the changeset
      // wrapper
      for (auto it = key_set.begin(); it != key_set.end(); it++) {
        (data->second).insert(*it);
      }

      zmq_util::send_msg((void*)data, &changeset_address_requester);
      zmq::message_t msg;
      zmq_util::recv_msg(&changeset_address_requester, msg);

      changeset_address* res = *(changeset_address **)(msg.data());
      send_gossip(res, cache, ip, thread_id);
      delete res;

      string shell_command;
      // remove the volume locally
      if (enable_ebs) {
        shell_command = "scripts/remove_volume.sh " + device_name + " " + to_string(thread_id);
      } else {
        shell_command = "scripts/remove_volume_dummy.sh " + device_name + " " + to_string(thread_id);
      }

      system(shell_command.c_str());
      zmq_util::send_string(device_name + ":" + to_string(thread_id), &cache[LOCAL_DEPART_DONE_ADDR]);
      
      // break because we have now successfully removed the volume and are
      // ending this thread's execution
      break;
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
        send_gossip(res, cache, ip, thread_id);
        delete res;
        local_changeset.clear();
      }

      gossip_start = std::chrono::system_clock::now();
    }

    storage_end = std::chrono::system_clock::now();
    // check whether we should send storage consumption update
    if (chrono::duration_cast<std::chrono::seconds>(storage_end-storage_start).count() >= STORAGE_CONSUMPTION_REPORT_THRESHOLD) {
      storage_data* data = new storage_data();

      data->first = to_string(thread_id);

      // compute total storage consumption
      size_t consumption = 0;
      for (auto it = size_map.begin(); it != size_map.end(); it++) {
        consumption += it->second;
      }
      data->second = consumption;

      zmq_util::send_msg((void*)data, &cache[LOCAL_STORAGE_CONSUMPTION_ADDR]);

      storage_start = std::chrono::system_clock::now();
    }
  }
}

void add_thread(map<string, int>& ebs_device_map, 
    unordered_map<int, string>& inverse_ebs_device_map, 
    vector<thread>& ebs_threads,
    ebs_hash_t& local_ebs_hash_ring,
    unordered_map<string, ebs_key_info>& placement,
    set<int>& active_thread_id,
    int next_thread_id,
    string ip,
    zmq::context_t& context,
    SocketCache& cache) {

  cout << "Adding a new thread.\n";
  string ebs_device_id;

  if (ebs_device_map.size() == 0) {
    ebs_device_id = "ba";
    ebs_device_map[ebs_device_id] = next_thread_id;
    inverse_ebs_device_map[next_thread_id] = ebs_device_id;
  } else {
    bool has_slot = false;
    for (auto it = ebs_device_map.begin(); it != ebs_device_map.end(); it++) {
      if (it->second == -1) {
        has_slot = true;
        ebs_device_id = it->first;

        ebs_device_map[it->first] = next_thread_id;
        inverse_ebs_device_map[next_thread_id] = it->first;
        break;
      }
    }

    if (!has_slot) {
      ebs_device_id = getNextDeviceID((ebs_device_map.rbegin())->first);
      ebs_device_map[ebs_device_id] = next_thread_id;
      inverse_ebs_device_map[next_thread_id] = ebs_device_id;
    }
  }

  cout << "Adding device with ID " + ebs_device_id + " and thread with ID " + to_string(next_thread_id) + ".\n";

  string shell_command;
  if (enable_ebs) {
    shell_command = "scripts/add_volume.sh " + ebs_device_id + " 10 " + to_string(next_thread_id);
  } else {
    shell_command = "scripts/add_volume_dummy.sh " + ebs_device_id + " 10 " + to_string(next_thread_id);
  }

  system(shell_command.c_str());
  ebs_threads.push_back(thread(ebs_worker_routine, &context, ip, next_thread_id));

  active_thread_id.insert(next_thread_id);
  local_ebs_hash_ring.insert(worker_node_t(ip, next_thread_id));

  // repartition data
  unordered_map<string, redistribution_address*> redistribution_map;

  string target_address = ip + ":" + to_string(SERVER_PORT + next_thread_id);
  for (auto it = placement.begin(); it != placement.end(); it++) {

    auto result = responsible<worker_node_t, ebs_hasher>(it->first, it->second.local_ebs_replication_, local_ebs_hash_ring, target_address);

    bool resp = result.first;

    if (resp) {
      cout << "The new thread is responsible for key " + it->first + ".\n";
      string sender_address = result.second->local_redistribute_addr_;
      cout << "The sender address is " + sender_address + ".\n";

      if (redistribution_map.find(sender_address) == redistribution_map.end()) {
        redistribution_map[sender_address] = new redistribution_address();
      }

      (*redistribution_map[sender_address])[target_address].insert(pair<string, bool>(it->first, true));
    }
  }

  for (auto it = redistribution_map.begin(); it != redistribution_map.end(); it++) {
    zmq_util::send_msg((void*)it->second, &cache[it->first]);
  }

  next_thread_id += 1;
}

void remove_thread(set<int>& active_thread_id,
    ebs_hash_t& local_ebs_hash_ring,
    unordered_map<int, string>& inverse_ebs_device_map,
    string ip,
    SocketCache& cache,
    unordered_map<string, size_t>& ebs_storage) {

  if (active_thread_id.rbegin() == active_thread_id.rend()) {
    cout << "Error: No remaining threads are running. Nothing to remove.\n";
  } else {
    int target_thread_id = *(active_thread_id.rbegin());
    cout << "Removing thread " + to_string(target_thread_id) + ".\n";

    worker_node_t wnode = worker_node_t(ip, target_thread_id);
    local_ebs_hash_ring.erase(wnode);
    active_thread_id.erase(target_thread_id);
    ebs_storage.erase(to_string(target_thread_id));

    string device_id = inverse_ebs_device_map[target_thread_id];
    zmq_util::send_string(device_id, &cache[wnode.local_depart_addr_]);
  }
}

int main(int argc, char* argv[]) {
  if (argc != 3) {
    cerr << "usage:" << argv[0] << " <new_node> <enable_ebs>" << endl;
    return 1;
  }

  if (string(argv[1]) != "y" && string(argv[1]) != "n") {
    cerr << "Invalid first argument: " << string(argv[1]) << "." << endl;
    return 1;
  }

  if (string(argv[2]) != "y" && string(argv[2]) != "n") {
    cerr << "Invalid second argument: " << string(argv[2]) << "." << endl;
    return 1;
  }

  string ip = get_ip("server");
  string new_node = argv[1];
  if (string(argv[2]) == "y") {
    enable_ebs = true;
  } else {
    enable_ebs = false;
  }

  master_node_t mnode = master_node_t(ip, "E");

  // prepare the zmq context
  zmq::context_t context(1);
  SocketCache cache(&context, ZMQ_PUSH);
  SocketCache key_address_requesters(&context, ZMQ_REQ);

  // create our hash rings
  global_hash_t global_ebs_hash_ring;
  ebs_hash_t local_ebs_hash_ring;
  unordered_map<string, ebs_key_info> placement;

  // keep track of the keys' hotness
  unordered_map<string, size_t> key_access_frequency;
  unordered_map<string, multiset<std::chrono::time_point<std::chrono::system_clock>>> key_access_monitoring;

  // keep track of the storage consumption for all ebs volumes
  unordered_map<string, size_t> ebs_storage;

  set<int> active_ebs_thread_id = set<int>();
  for (int i = 1; i <= EBS_THREAD_NUM; i++) {
    active_ebs_thread_id.insert(i);
  }

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

  // read server address from the file
  if (new_node == "n") {
    address.open("conf/server/start_servers.txt");
    // add itself to the ring
    global_ebs_hash_ring.insert(mnode);

    // add all other servers
    while (getline(address, ip_line)) {
      global_ebs_hash_ring.insert(master_node_t(ip_line, "E"));
    }
    address.close();
  } else { // get server address from the seed node
    address.open("conf/server/seed_server.txt");
    getline(address, ip_line);
    address.close();

    // tell the seed node that you are joining
    zmq::socket_t addr_requester(context, ZMQ_REQ);
    addr_requester.connect(master_node_t(ip_line, "E").seed_connection_connect_addr_);
    zmq_util::send_string("join", &addr_requester);

    // receive and add all the addresses that seed node sent
    vector<string> addresses;
    split(zmq_util::recv_string(&addr_requester), '|', addresses);
    for (auto it = addresses.begin(); it != addresses.end(); it++) {
      global_ebs_hash_ring.insert(master_node_t(*it, "E"));
    }

    // add itself to global hash ring
    global_ebs_hash_ring.insert(mnode);
  }

  // this map is ordered, so that we can figure out the device name when adding
  // new devices
  map<string, int> ebs_device_map;

  unordered_map<int, string> inverse_ebs_device_map;
  vector<thread> ebs_threads;

  // this is the smallest device name that we can have because of EBS
  // idiosyncrasies
  string eid = "ba";

  // create the initial volumes and start the initial threads based on
  // EBS_THREAD_NUM
  for (int thread_id = 1; thread_id <= EBS_THREAD_NUM; thread_id++) {
    ebs_device_map[eid] = thread_id;
    inverse_ebs_device_map[thread_id] = eid;

    cout << "device id to be added is " + eid + "\n";
    cout << "thread id is " + to_string(thread_id) + "\n";

    string shell_command;
    if (enable_ebs) {
      shell_command = "scripts/add_volume.sh " + eid + " 10 " + to_string(thread_id);
    }
    else {
      shell_command = "scripts/add_volume_dummy.sh " + eid + " 10 " + to_string(thread_id);
    }

    system(shell_command.c_str());
    eid = getNextDeviceID(eid);
    ebs_threads.push_back(thread(ebs_worker_routine, &context, ip, thread_id));
    local_ebs_hash_ring.insert(worker_node_t(ip, thread_id));
  }

  set<int> active_thread_id = set<int>();
  for (int i = 1; i <= EBS_THREAD_NUM; i++) {
    active_thread_id.insert(i);
  }

  if (new_node == "y") {
    // notify other servers that there is a new node
    for (auto it = global_ebs_hash_ring.begin(); it != global_ebs_hash_ring.end(); it++) {
      if (it->second.ip_.compare(ip) != 0) {
        zmq_util::send_string(ip, &cache[(it->second).node_join_connect_addr_]);
      }
    }
  }

  // notify proxies that this node has joined the service
  for (auto it = proxy_address.begin(); it != proxy_address.end(); it++) {
    zmq_util::send_string("join:E:" + ip, &cache[proxy_node_t(*it).notify_connect_addr_]);
  }

  // notify monitoring nodes that this node has joined the service
  for (auto it = monitoring_address.begin(); it != monitoring_address.end(); it++) {
    zmq_util::send_string("join:E:" + ip, &cache[monitoring_node_t(*it).notify_connect_addr_]);
  }

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

  // responsible for monitoring when a worker has finished departing
  zmq::socket_t depart_done_puller(context, ZMQ_PULL);
  depart_done_puller.bind(LOCAL_DEPART_DONE_ADDR);

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
                                       {static_cast<void*>(depart_done_puller), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(self_depart_responder), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(replication_factor_change_puller), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(storage_consumption_puller), 0, ZMQ_POLLIN, 0}
                                     };

  int next_thread_id = EBS_THREAD_NUM + 1;

  auto storage_start = std::chrono::system_clock::now();
  auto storage_end = std::chrono::system_clock::now();
  auto hotness_start = std::chrono::system_clock::now();
  auto hotness_end = std::chrono::system_clock::now();

  // enter event loop
  while (true) {
    zmq_util::poll(0, &pollitems);

    if (pollitems[0].revents & ZMQ_POLLIN) {
      cout << "Received an address request.\n";
      string request = zmq_util::recv_string(&addr_responder);

      string addresses;
      for (auto it = global_ebs_hash_ring.begin(); it != global_ebs_hash_ring.end(); it++) {
        addresses += (it->second.ip_ + "|");
      }

      // remove the trailing pipe
      addresses.pop_back();
      zmq_util::send_string(addresses, &addr_responder);
    }

    if (pollitems[1].revents & ZMQ_POLLIN) {
      string new_server_ip = zmq_util::recv_string(&join_puller);
      cout << "Received a node join. New node is " << new_server_ip << ".\n";
      master_node_t new_node = master_node_t(new_server_ip, "E");

      // update global hash ring
      global_ebs_hash_ring.insert(new_node);

      // a map for each local worker that has to redistribute to the remote
      // node
      unordered_map<string, redistribution_address*> redistribution_map;

      // for each key in this set, we will ask the new node which worker the
      // key should be sent to
      unordered_set<string> key_to_query;

      // iterate through all of my keys
      for (auto it = placement.begin(); it != placement.end(); it++) {

        string key = it->first;
        ebs_key_info info = it->second;

        auto result = responsible<master_node_t, crc32_hasher>(key, info.global_ebs_replication_, global_ebs_hash_ring, new_node.id_);

        if (result.first && (result.second->ip_.compare(ip) == 0)) {
          key_to_query.insert(it->first);
        }
      }

      communication::Key_Response resp = get_key_address<ebs_key_info>(new_node.key_exchange_connect_addr_, "", key_to_query, key_address_requesters, placement);

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
          auto pos = local_ebs_hash_ring.find(key);
          for (int k = 0; k < placement[key].local_ebs_replication_; k++) {
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
            if (++pos == local_ebs_hash_ring.end()) {
              pos = local_ebs_hash_ring.begin();
            }
          }
        }
      }

      // send a local message for each local worker that has to redistribute
      // data to a remote node
      for (auto it = redistribution_map.begin(); it != redistribution_map.end(); it++) {
        zmq_util::send_msg((void*)it->second, &cache[it->first]);
      }
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      string departing_server_ip = zmq_util::recv_string(&depart_puller);
      cout << "Received departure for node " << departing_server_ip << ".\n";

      // update hash ring
      global_ebs_hash_ring.erase(master_node_t(departing_server_ip, "E"));
    }

    if (pollitems[3].revents & ZMQ_POLLIN) {
      cout << "Received a key address request.\n";
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
          placement[key] = ebs_key_info(gmr, ger);
        }

        // check if the node is responsible for this key
        auto result = responsible<master_node_t, crc32_hasher>(key, placement[key].global_ebs_replication_, global_ebs_hash_ring, mnode.id_);

        if (result.first) {
          if (sender == "proxy") {
            // update key access monitoring map
            key_access_monitoring[key].insert(std::chrono::system_clock::now());
          }

          communication::Key_Response_Tuple* tp = res.add_tuple();
          tp->set_key(key);

          // find all the local worker threads that are assigned to this key
          auto it = local_ebs_hash_ring.find(key);
          for (int i = 0; i < placement[key].local_ebs_replication_; i++) {
            // add each one to the response
            communication::Key_Response_Address* tp_addr = tp->add_address();
            if (sender == "server") {
              tp_addr->set_addr(it->second.id_);
            } else {
              tp_addr->set_addr(it->second.proxy_connection_connect_addr_);
            }

            if (++it == local_ebs_hash_ring.end()) {
              it = local_ebs_hash_ring.begin();
            }
          }
        }

        string response;
        res.SerializeToString(&response);
        zmq_util::send_string(response, &key_address_responder);
      } 
    }

    if (pollitems[4].revents & ZMQ_POLLIN) {
      cout << "Received a changeset address request from a worker thread.\n";

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
        // first, check the local ebs ring
        auto ebs_pos = local_ebs_hash_ring.find(key);
        for (int i = 0; i < placement[key].local_ebs_replication_; i++) {
          // add any thread that is responsible for this key but is not the
          // thread that made the request
          if (ebs_pos->second.id_.compare(self_id) != 0) {
            (*res)[ebs_pos->second.id_].insert(key);
          }

          if (++ebs_pos == local_ebs_hash_ring.end()) {
            ebs_pos = local_ebs_hash_ring.begin();
          }
        }

        // second, check the global ebs ring for any nodes that might also be
        // responsible for this key
        auto pos = global_ebs_hash_ring.find(key);
        for (int i = 0; i < placement[key].global_ebs_replication_; i++) {
          if (pos->second.ip_.compare(ip) != 0) {
            node_map[pos->second].insert(key);
          }

          if (++pos == global_ebs_hash_ring.end()) {
            pos = global_ebs_hash_ring.begin();
          }
        }

        // finally, check if the key has replica on the memory tier
        if (placement[key].global_memory_replication_ != 0) {
          key_to_proxy.insert(key);
        }
      }

      // for any remote nodes that should receive gossip, we make key
      // requests
      for (auto map_iter = node_map.begin(); map_iter != node_map.end(); map_iter++) {
        // get key address
        communication::Key_Response resp = get_key_address<ebs_key_info>(map_iter->first.key_exchange_connect_addr_, "", map_iter->second, key_address_requesters, placement);

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
        communication::Key_Response resp = get_key_address<ebs_key_info>(target_proxy_address, "M", key_to_proxy, key_address_requesters, placement);

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
      // we wait for this message to come back from the worker thread because
      // we want to avoid a race condition where the master thread tries to
      // reuse the volume name before it's removed
      vector<string> v;
      split(zmq_util::recv_string(&depart_done_puller), ':', v);
      cout << "Thread " + v[1] + " has departed.\n";

      ebs_device_map[v[0]] = -1;
      inverse_ebs_device_map.erase(stoi(v[1]));
    }

    if (pollitems[6].revents & ZMQ_POLLIN) {
      cout << "Node is departing.\n";

      global_ebs_hash_ring.erase(mnode);
      for (auto it = global_ebs_hash_ring.begin(); it != global_ebs_hash_ring.end(); it++) {
        zmq_util::send_string(ip, &cache[it->second.node_depart_connect_addr_]);
      }
      
      // notify proxies
      for (auto it = proxy_address.begin(); it != proxy_address.end(); it++) {
        zmq_util::send_string("depart:E:" + ip, &cache[proxy_node_t(*it).notify_connect_addr_]);
      }

      // form the key_request map
      unordered_map<string, communication::Key_Request> key_request_map;
      for (auto it = placement.begin(); it != placement.end(); it++) {
        string key = it->first;
        auto pos = global_ebs_hash_ring.find(key);

        for (int i = 0; i < placement[key].global_ebs_replication_; i++) {
          key_request_map[pos->second.key_exchange_connect_addr_].set_sender("server");
          communication::Key_Request_Tuple* tp = key_request_map[pos->second.key_exchange_connect_addr_].add_tuple();
          tp->set_key(key);
          tp->set_global_memory_replication(placement[key].global_memory_replication_);
          tp->set_global_ebs_replication(placement[key].global_ebs_replication_);

          if (++pos == global_ebs_hash_ring.end()) {
            pos = global_ebs_hash_ring.begin();
          }
        }
      }

      // send key address requests to other server nodes
      unordered_map<string, redistribution_address*> redistribution_map;
      for (auto it = key_request_map.begin(); it != key_request_map.end(); it++) {
        string key_req;
        it->second.SerializeToString(&key_req);
        zmq_util::send_string(key_req, &key_address_requesters[it->first]);

        string key_res = zmq_util::recv_string(&key_address_requesters[it->first]);
        communication::Key_Response resp;
        resp.ParseFromString(key_res);

        for (int i = 0; i < resp.tuple_size(); i++) {
          for (int j = 0; j < resp.tuple(i).address_size(); j++) {
            string key = resp.tuple(i).key();
            string target_address = resp.tuple(i).address(j).addr();
            auto pos = local_ebs_hash_ring.find(key);

            for (int k = 0; k < placement[key].local_ebs_replication_; k++) {
              string worker_address = pos->second.local_redistribute_addr_;
              if (redistribution_map.find(worker_address) == redistribution_map.end()) {
                redistribution_map[worker_address] = new redistribution_address();
              }

              (*redistribution_map[worker_address])[target_address].insert(pair<string, bool>(key, false));

              if (++pos == local_ebs_hash_ring.end()) {
                pos = local_ebs_hash_ring.begin();
              }
            }
          }
        }
      }

      for (auto it = redistribution_map.begin(); it != redistribution_map.end(); it++) {
        zmq_util::send_msg((void*)it->second, &cache[it->first]);
      }

      cout << "Killing all EBS instances.\n";

      for (auto it = inverse_ebs_device_map.begin(); it != inverse_ebs_device_map.end(); it++) {
        active_thread_id.erase(it->first);
        local_ebs_hash_ring.erase(worker_node_t(ip, it->first));
        string shell_command;
        if (enable_ebs) {
          shell_command = "scripts/remove_volume.sh " + it->second + " " + to_string(it->first);
        } else {
          shell_command = "scripts/remove_volume_dummy.sh " + it->second + " " + to_string(it->first);
        }

        system(shell_command.c_str());
      }
      
      // TODO: once we break here, I don't think that the threads will have
      // finished. they will still be looping.
      break;
    }

    if (pollitems[7].revents & ZMQ_POLLIN) {
      cout << "Received replication factor change request\n";

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
          placement[key] = ebs_key_info(gmr, ger);
        }

        // first, check whether the node is responsible for the key before the replication factor change
        auto result = responsible<master_node_t, crc32_hasher>(key, placement[key].global_ebs_replication_, global_ebs_hash_ring, mnode.id_);
        // update placement info
        placement[key].global_memory_replication_ = gmr;
        placement[key].global_ebs_replication_ = ger;
        // proceed only if the node is responsible for the key before the replication factor change
        if (result.first) {
          // check the global ring and request worker addresses from other node's master thread
          auto pos = global_ebs_hash_ring.find(key);
          for (int i = 0; i < placement[key].global_ebs_replication_; i++) {
            if (pos->second.ip_.compare(ip) != 0) {
              node_map[pos->second].insert(key);
            }
            if (++pos == global_ebs_hash_ring.end()) {
              pos = global_ebs_hash_ring.begin();
            }
          }

          // check if the key has replica on the memory tier
          if (placement[key].global_memory_replication_ != 0) {
            key_to_proxy.insert(key);
          }

          // check if the key has to be removed under the new replication factor
          result = responsible<master_node_t, crc32_hasher>(key, placement[key].global_ebs_replication_, global_ebs_hash_ring, mnode.id_);
          if (result.first) {
            key_remove_map[key] = false;
          } else {
            key_remove_map[key] = true;
          }
        }
      }

      // initialize the redistribution map (key is the address of ebs worker thread)
      unordered_map<string, redistribution_address*> redistribution_map;

      for (auto map_iter = node_map.begin(); map_iter != node_map.end(); map_iter++) {
        // get key address
        communication::Key_Response resp = get_key_address<ebs_key_info>(map_iter->first.key_exchange_connect_addr_, "", map_iter->second, key_address_requesters, placement);
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
            auto pos = local_ebs_hash_ring.find(key);
            for (int k = 0; k < placement[key].local_ebs_replication_; k++) {
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
              if (++pos == local_ebs_hash_ring.end()) {
                pos = local_ebs_hash_ring.begin();
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
        communication::Key_Response resp = get_key_address<ebs_key_info>(target_proxy_address, "M", key_to_proxy, key_address_requesters, placement);

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
            auto pos = local_ebs_hash_ring.find(key);
            for (int k = 0; k < placement[key].local_ebs_replication_; k++) {
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
              if (++pos == local_ebs_hash_ring.end()) {
                pos = local_ebs_hash_ring.begin();
              }
            }
          }
        }
      }

      // send a local message for each local worker that has to redistribute
      // data to a remote node
      for (auto it = redistribution_map.begin(); it != redistribution_map.end(); it++) {
        zmq_util::send_msg((void*)it->second, &cache[it->first]);
      }
    }

    if (pollitems[8].revents & ZMQ_POLLIN) {
      cerr << "received storage update from worker thread\n";
      zmq::message_t msg;
      zmq_util::recv_msg(&storage_consumption_puller, msg);
      storage_data* data = *(storage_data **)(msg.data());

      ebs_storage[data->first] = data->second;

      delete data;
    }

    storage_end = std::chrono::system_clock::now();
    hotness_end = std::chrono::system_clock::now();

    if (chrono::duration_cast<std::chrono::seconds>(storage_end-storage_start).count() >= STORAGE_CONSUMPTION_REPORT_THRESHOLD) {
      communication::Storage_Update su;
      su.set_node_ip(mnode.ip_);
      su.set_node_type("E");

      for (auto it = ebs_storage.begin(); it != ebs_storage.end(); it++) {
        communication::Storage_Update_EBS* e = su.add_ebs();
        e->set_id(it->first);
        e->set_storage(it->second);
      }
      string msg;
      su.SerializeToString(&msg);
      // send the storage consumption update
      zmq_util::send_string(msg, &cache[monitoring_node.storage_consumption_connect_addr_]);

      storage_start = std::chrono::system_clock::now();
    }

    if (chrono::duration_cast<std::chrono::seconds>(hotness_end-hotness_start).count() >= MONITORING_PERIOD) {
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

      // send hotness info to the monitoring node
      communication::Key_Hotness_Update khu;
      khu.set_node_ip(mnode.ip_);
      for (auto it = key_access_frequency.begin(); it != key_access_frequency.end(); it++) {
        communication::Key_Hotness_Update_Tuple* tp = khu.add_tuple();
        tp->set_key(it->first);
        tp->set_access(it->second);
      }
      string msg;
      khu.SerializeToString(&msg);
      zmq_util::send_string(msg, &cache[monitoring_node.key_hotness_connect_addr_]);
      
      hotness_start = std::chrono::system_clock::now();
    }
  }
}
