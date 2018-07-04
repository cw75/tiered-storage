#ifndef __COMMON_H__
#define __COMMON_H__

#include <atomic>
#include <string>
#include <functional>
#include "utils/consistent_hash_map.hpp"
#include "communication.pb.h"
#include "zmq/socket_cache.h"
#include "zmq/zmq_util.h"

#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"

using namespace std;

// Define metadata identifier
#define METADATA_IDENTIFIER "BEDROCKMETADATA"

// Define server report threshold (in second)
#define SERVER_REPORT_THRESHOLD 15
// Define server's key monitoring threshold (in second)
#define KEY_MONITORING_THRESHOLD 60
// Define monitoring threshold (in second)
#define MONITORING_THRESHOLD 30
// Define the threshold for retry rep factor query for gossip handling (in second)
#define RETRY_THRESHOLD 10
// Define the grace period for triggering elasticity action (in second)
#define GRACE_PERIOD 120

// Define the replication factor for the metadata
#define METADATA_REPLICATION_FACTOR 2

// Define the default replication factor for the data
#define DEFAULT_GLOBAL_MEMORY_REPLICATION 1
#define DEFAULT_GLOBAL_EBS_REPLICATION 0
#define MINIMUM_REPLICA_NUMBER 1

// Define the default local replication factor
#define DEFAULT_LOCAL_REPLICATION 1

// Define the number of memory threads
#define MEMORY_THREAD_NUM 4

// Define the number of ebs threads
#define EBS_THREAD_NUM 4

// Define the number of routing worker threads
#define PROXY_THREAD_NUM 4

// Define the number of benchmark threads
#define BENCHMARK_THREAD_NUM 16

// Define the number of virtual thread per each physical thread
#define VIRTUAL_THREAD_NUM 3000

#define MEM_CAPACITY_MAX 0.6
#define MEM_CAPACITY_MIN 0.3
#define EBS_CAPACITY_MAX 0.75
#define EBS_CAPACITY_MIN 0.5

#define PROMOTE_THRESHOLD 0
#define DEMOTE_THRESHOLD 1

#define MIN_TIER 1
#define MAX_TIER 2

#define MINIMUM_MEMORY_NODE 12
#define MINIMUM_EBS_NODE 0

#define SLO_WORST 3000
#define SLO_BEST 1500

#define HOT_KEY_THRESHOLD 5000

// node capacity in KB
unsigned MEM_NODE_CAPACITY = 60000000;
unsigned EBS_NODE_CAPACITY = 256000000;

// value size in KB
#define VALUE_SIZE 256

// define server base ports
#define SERVER_PORT 6000
#define NODE_JOIN_BASE_PORT 6050
#define NODE_DEPART_BASE_PORT 6100
#define SELF_DEPART_BASE_PORT 6150
#define SERVER_REPLICATION_FACTOR_BASE_PORT 6200
#define SERVER_REQUEST_PULLING_BASE_PORT 6250
#define GOSSIP_BASE_PORT 6300
#define SERVER_REPLICATION_FACTOR_CHANGE_BASE_PORT 6350

// define routing base ports
#define SEED_BASE_PORT 6400
#define ROUTING_NOTIFY_BASE_PORT 6450
#define ROUTING_KEY_ADDRESS_BASE_PORT 6500
#define ROUTING_REPLICATION_FACTOR_BASE_PORT 6550
#define ROUTING_REPLICATION_FACTOR_CHANGE_BASE_PORT 6600

// used by monitoring nodes
#define MON_NOTIFY_BASE_PORT 6650
#define MON_REQUEST_PULLING_BASE_PORT 6700
#define DEPART_DONE_BASE_PORT 6750
#define LATENCY_REPORT_BASE_PORT 6800

// used by user nodes
#define USER_REQUEST_PULLING_BASE_PORT 6850
#define USER_KEY_ADDRESS_BASE_PORT 6900
#define COMMAND_BASE_PORT 6950

// server thread
class server_thread_t {
  string ip_;
  unsigned tid_;
  unsigned virtual_num_;

public:
  server_thread_t() {}
  server_thread_t(string ip, unsigned tid): ip_(ip), tid_(tid) {}
  server_thread_t(string ip, unsigned tid, unsigned virtual_num): ip_(ip), tid_(tid), virtual_num_(virtual_num) {}

  string get_ip() const {
    return ip_;
  }
  unsigned get_tid() const {
    return tid_;
  }
  unsigned get_virtual_num() const {
    return virtual_num_;
  }
  string get_id() const {
    return ip_ + ":" + to_string(SERVER_PORT + tid_);
  }
  string get_virtual_id() const {
    return ip_ + ":" + to_string(SERVER_PORT + tid_) + "_" + to_string(virtual_num_);
  }
  string get_node_join_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + NODE_JOIN_BASE_PORT);
  }
  string get_node_join_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + NODE_JOIN_BASE_PORT);
  }
  string get_node_depart_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + NODE_DEPART_BASE_PORT);
  }
  string get_node_depart_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + NODE_DEPART_BASE_PORT);
  }
  string get_self_depart_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + SELF_DEPART_BASE_PORT);
  }
  string get_self_depart_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + SELF_DEPART_BASE_PORT);
  }
  string get_request_pulling_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + SERVER_REQUEST_PULLING_BASE_PORT);
  }
  string get_request_pulling_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + SERVER_REQUEST_PULLING_BASE_PORT);
  }
  string get_replication_factor_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + SERVER_REPLICATION_FACTOR_BASE_PORT);
  }
  string get_replication_factor_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + SERVER_REPLICATION_FACTOR_BASE_PORT);
  }
  string get_gossip_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + GOSSIP_BASE_PORT);
  }
  string get_gossip_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + GOSSIP_BASE_PORT);
  }
  string get_replication_factor_change_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + SERVER_REPLICATION_FACTOR_CHANGE_BASE_PORT);
  }
  string get_replication_factor_change_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + SERVER_REPLICATION_FACTOR_CHANGE_BASE_PORT);
  }
};

bool operator==(const server_thread_t& l, const server_thread_t& r) {
  if (l.get_id().compare(r.get_id()) == 0) {
    return true;
  } else {
    return false;
  }
}

struct thread_hash {
  std::size_t operator () (const server_thread_t &st) const {
    return std::hash<string>{}(st.get_id());
  }
};

struct global_hasher {
  uint32_t operator()(const server_thread_t& th) {
    // prepend a string to make the hash value different than
    // what it would be on the naked input
    return std::hash<string>{}("GLOBAL"+th.get_virtual_id());
  }

  uint32_t operator()(const string& key) {
    // prepend a string to make the hash value different than
    // what it would be on the naked input
    return std::hash<string>{}("GLOBAL"+key);
  }

  typedef uint32_t result_type;
};

struct local_hasher {
  hash<string>::result_type operator()(const server_thread_t& th) {
    return hash<string>{}(to_string(th.get_tid()) + "_" + to_string(th.get_virtual_num()));
  }

  hash<string>::result_type operator()(const string& key) {
    return hash<string>{}(key);
  }

  typedef hash<string>::result_type result_type;
};


// routing thread
class routing_thread_t {
  string ip_;
  unsigned tid_;

public:
  routing_thread_t() {}
  routing_thread_t(string ip, unsigned tid): ip_(ip), tid_(tid) {}

  string get_ip() const {
    return ip_;
  }

  unsigned get_tid() const {
    return tid_;
  }

  string get_seed_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + SEED_BASE_PORT);
  }

  string get_seed_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + SEED_BASE_PORT);
  }

  string get_notify_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + ROUTING_NOTIFY_BASE_PORT);
  }

  string get_notify_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + ROUTING_NOTIFY_BASE_PORT);
  }

  string get_key_address_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + ROUTING_KEY_ADDRESS_BASE_PORT);
  }

  string get_key_address_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + ROUTING_KEY_ADDRESS_BASE_PORT);
  }

  string get_replication_factor_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + ROUTING_REPLICATION_FACTOR_BASE_PORT);
  }

  string get_replication_factor_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + ROUTING_REPLICATION_FACTOR_BASE_PORT);
  }

  string get_replication_factor_change_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + ROUTING_REPLICATION_FACTOR_CHANGE_BASE_PORT);
  }

  string get_replication_factor_change_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + ROUTING_REPLICATION_FACTOR_CHANGE_BASE_PORT);
  }
};


// monitoring thread
class monitoring_thread_t {
  string ip_;

public:
  monitoring_thread_t() {}
  monitoring_thread_t(string ip): ip_(ip) {}

  string get_ip() const {
    return ip_;
  }

  string get_notify_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(MON_NOTIFY_BASE_PORT);
  }

  string get_notify_bind_addr() const {
    return "tcp://*:" + to_string(MON_NOTIFY_BASE_PORT);
  }

  string get_request_pulling_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(MON_REQUEST_PULLING_BASE_PORT);
  }

  string get_request_pulling_bind_addr() const {
    return "tcp://*:" + to_string(MON_REQUEST_PULLING_BASE_PORT);
  }

  string get_depart_done_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(DEPART_DONE_BASE_PORT);
  }

  string get_depart_done_bind_addr() const {
    return "tcp://*:" + to_string(DEPART_DONE_BASE_PORT);
  }

  string get_latency_report_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(LATENCY_REPORT_BASE_PORT);
  }

  string get_latency_report_bind_addr() const {
    return "tcp://*:" + to_string(LATENCY_REPORT_BASE_PORT);
  }
};

class user_thread_t {
  string ip_;
  unsigned tid_;

public:
  user_thread_t() {}
  user_thread_t(string ip, unsigned tid): ip_(ip), tid_(tid) {}

  string get_ip() const {
    return ip_;
  }

  unsigned get_tid() const {
    return tid_;
  }

  string get_request_pulling_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + USER_REQUEST_PULLING_BASE_PORT);
  }

  string get_request_pulling_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + USER_REQUEST_PULLING_BASE_PORT);
  }

  string get_key_address_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + USER_KEY_ADDRESS_BASE_PORT);
  }

  string get_key_address_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + USER_KEY_ADDRESS_BASE_PORT);
  }
};

// represents the replication state for each key
struct key_info {
  unordered_map<unsigned, unsigned> global_replication_map_;
  unordered_map<unsigned, unsigned> local_replication_map_;
};

// per-tier metadata
struct tier_data {
  tier_data() : thread_number_(1), default_replication_(1), node_capacity_(0) {}

  tier_data(unsigned t_num, unsigned rep, unsigned long long node_capacity)
    : thread_number_(t_num), default_replication_(rep), node_capacity_(node_capacity) {}

  unsigned thread_number_;

  unsigned default_replication_;

  unsigned long long node_capacity_;
};

typedef consistent_hash_map<server_thread_t, global_hasher> global_hash_t;
typedef consistent_hash_map<server_thread_t, local_hasher> local_hash_t;

void split(const string &s, char delim, vector<string> &elems) {
  stringstream ss(s);
  string item;

  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
}

// assuming the replication factor will never be greater than the number of nodes in a tier
// return a set of server_thread_t that are responsible for a key
unordered_set<server_thread_t, thread_hash> responsible_global(string key, unsigned global_rep, global_hash_t& global_hash_ring) {
  unordered_set<server_thread_t, thread_hash> threads;
  auto pos = global_hash_ring.find(key);

  if (pos != global_hash_ring.end()) {
    // iterate for every value in the replication factor
    unsigned i = 0;

    while (i < global_rep && i != global_hash_ring.size() / VIRTUAL_THREAD_NUM) {
      bool succeed = threads.insert(pos->second).second;
      if (++pos == global_hash_ring.end()) {
        pos = global_hash_ring.begin();
      }

      if (succeed) {
        i += 1;
      }
    }
  }

  return threads;
}

// assuming the replication factor will never be greater than the number of worker threads
// return a set of tids that are responsible for a key
unordered_set<unsigned> responsible_local(string key, unsigned local_rep, local_hash_t& local_hash_ring) {
  unordered_set<unsigned> tids;
  auto pos = local_hash_ring.find(key);

  if (pos != local_hash_ring.end()) {
    // iterate for every value in the replication factor
    unsigned i = 0;

    while (i < local_rep && i != local_hash_ring.size() / VIRTUAL_THREAD_NUM) {
      bool succeed = tids.insert(pos->second.get_tid()).second;
      if (++pos == local_hash_ring.end()) {
        pos = local_hash_ring.begin();
      }

      if (succeed) {
        i += 1;
      }
    }
  }

  return tids;
}

void prepare_get_tuple(communication::Request& req, string key) {
  communication::Request_Tuple* tp = req.add_tuple();
  tp->set_key(key);
}

void prepare_put_tuple(communication::Request& req, string key, string value, unsigned long long timestamp) {
  communication::Request_Tuple* tp = req.add_tuple();
  tp->set_key(key);
  tp->set_value(value);
  tp->set_timestamp(timestamp);
}

template<typename REQ, typename RES>
bool recursive_receive(zmq::socket_t& receiving_socket, zmq::message_t& message, REQ& req, RES& response, bool& succeed) {
  bool rc = receiving_socket.recv(&message);

  if (rc) {
    auto serialized_resp = zmq_util::message_to_string(message);
    response.ParseFromString(serialized_resp);

    if (req.request_id() == response.response_id()) {
      succeed = true;
      return false;
    } else {
      return true;
    }
  } else {
    // timeout
    if (errno == EAGAIN) {
      succeed = false;
    } else {
      succeed = false;
    }

    return false;
  }
}

template<typename REQ, typename RES>
RES send_request(REQ& req, zmq::socket_t& sending_socket, zmq::socket_t& receiving_socket, bool& succeed) {
  string serialized_req;
  req.SerializeToString(&serialized_req);
  zmq_util::send_string(serialized_req, &sending_socket);

  RES response;
  zmq::message_t message;

  bool recurse = recursive_receive<REQ, RES>(receiving_socket, message, req, response, succeed);

  while (recurse) {
    response.Clear();
    zmq::message_t message;
    recurse = recursive_receive<REQ, RES>(receiving_socket, message, req, response, succeed);
  }

  return response;
}

void push_request(communication::Request& req, zmq::socket_t& socket) {
  string serialized_req;
  req.SerializeToString(&serialized_req);
  zmq_util::send_string(serialized_req, &socket);
}

template<typename H>
bool insert_to_hash_ring(H& hash_ring, string ip, unsigned tid) {
  bool succeed;

  for (unsigned virtual_num = 0; virtual_num < VIRTUAL_THREAD_NUM; virtual_num++) {
    succeed = hash_ring.insert(server_thread_t(ip, tid, virtual_num)).second;
  }

  return succeed;
}

template<typename H>
void remove_from_hash_ring(H& hash_ring, string ip, unsigned tid) {
  for (unsigned virtual_num = 0; virtual_num < VIRTUAL_THREAD_NUM; virtual_num++) {
    hash_ring.erase(server_thread_t(ip, tid, virtual_num));
  }
}

bool is_metadata(string key) {
  vector<string> v;
  split(key, '_', v);

  if (v[0] == "BEDROCKMETADATA") {
    return true;
  } else {
    return false;
  }
}

unordered_set<server_thread_t, thread_hash> get_responsible_threads_metadata(
    string& key,
    global_hash_t& global_memory_hash_ring,
    local_hash_t& local_memory_hash_ring) {

  unordered_set<server_thread_t, thread_hash> threads;
  auto mts = responsible_global(key, METADATA_REPLICATION_FACTOR, global_memory_hash_ring);

  for (auto it = mts.begin(); it != mts.end(); it++) {
    string ip = it->get_ip();
    auto tids = responsible_local(key, DEFAULT_LOCAL_REPLICATION, local_memory_hash_ring);

    for (auto iter = tids.begin(); iter != tids.end(); iter++) {
      threads.insert(server_thread_t(ip, *iter));
    }
  }

  return threads;
}

void issue_replication_factor_request(
    const string& respond_address,
    const string& key,
    global_hash_t& global_memory_hash_ring,
    local_hash_t& local_memory_hash_ring,
    SocketCache& pushers,

    unsigned& seed) {
  string key_rep = string(METADATA_IDENTIFIER) + "_" + key + "_replication";
  auto threads = get_responsible_threads_metadata(key_rep, global_memory_hash_ring, local_memory_hash_ring);

  if (threads.size() == 0) {
    cerr << "error!\n";
  }

  string target_address = next(begin(threads), rand_r(&seed) % threads.size())->get_request_pulling_connect_addr();

  communication::Request req;
  req.set_type("GET");
  req.set_respond_address(respond_address);

  prepare_get_tuple(req, string(METADATA_IDENTIFIER) + "_" + key + "_replication");
  push_request(req, pushers[target_address]);
}

// get all threads responsible for a key from the "node_type" tier
// metadata flag = 0 means the key is a metadata. Otherwise, it is a regular data
unordered_set<server_thread_t, thread_hash> get_responsible_threads(
    string respond_address,
    string key,
    bool metadata,
    unordered_map<unsigned, global_hash_t>& global_hash_ring_map,
    unordered_map<unsigned, local_hash_t>& local_hash_ring_map,
    unordered_map<string, key_info>& placement,
    SocketCache& pushers,
    vector<unsigned>& tier_ids,
    bool& succeed,
    unsigned& seed) {

  if (metadata) {
    succeed = true;
    return get_responsible_threads_metadata(key, global_hash_ring_map[1], local_hash_ring_map[1]);
  } else {
    unordered_set<server_thread_t, thread_hash> result;

    if (placement.find(key) == placement.end()) {
      issue_replication_factor_request(respond_address, key, global_hash_ring_map[1], local_hash_ring_map[1], pushers, seed);
      succeed = false;
    } else {
      for (auto id_iter = tier_ids.begin(); id_iter != tier_ids.end(); id_iter++) {
        unsigned tier_id = *id_iter;
        auto mts = responsible_global(key, placement[key].global_replication_map_[tier_id], global_hash_ring_map[tier_id]);

        for (auto it = mts.begin(); it != mts.end(); it++) {
          string ip = it->get_ip();
          auto tids = responsible_local(key, placement[key].local_replication_map_[tier_id], local_hash_ring_map[tier_id]);

          for (auto iter = tids.begin(); iter != tids.end(); iter++) {
            result.insert(server_thread_t(ip, *iter));
          }
        }
      }

      succeed = true;
    }

    return result;
  }
}

// query the routing for a key and return all address
vector<string> get_address_from_routing(
    user_thread_t& ut,
    string key,
    zmq::socket_t& sending_socket,
    zmq::socket_t& receiving_socket,
    bool& succeed,
    string& ip,
    unsigned& thread_id,
    unsigned& rid) {

  communication::Key_Request key_req;
  key_req.set_respond_address(ut.get_key_address_connect_addr());
  key_req.add_keys(key);

  string req_id = ip + ":" + to_string(thread_id) + "_" + to_string(rid);
  key_req.set_request_id(req_id);
  rid += 1;

  // query routing for addresses on the other tier
  auto key_response = send_request<communication::Key_Request, communication::Key_Response>(key_req, sending_socket, receiving_socket, succeed);
  vector<string> result;

  if (succeed) {
    for (int j = 0; j < key_response.tuple(0).addresses_size(); j++) {
      result.push_back(key_response.tuple(0).addresses(j));
    }
  }

  return result;
}

routing_thread_t get_random_routing_thread(vector<string>& routing_address, unsigned& seed) {
  string routing_ip = routing_address[rand_r(&seed) % routing_address.size()];
  unsigned tid = rand_r(&seed) % PROXY_THREAD_NUM;
  return routing_thread_t(routing_ip, tid);
}

void warmup(unordered_map<string, key_info>& placement) {
  for (unsigned i = 1; i <= 1000000; i++) {
    // key is 8 bytes
    string key = string(8 - to_string(i).length(), '0') + to_string(i);
    placement[key].global_replication_map_[1] = DEFAULT_GLOBAL_MEMORY_REPLICATION;
    placement[key].global_replication_map_[2] = DEFAULT_GLOBAL_EBS_REPLICATION;
    placement[key].local_replication_map_[1] = DEFAULT_LOCAL_REPLICATION;
    placement[key].local_replication_map_[2] = DEFAULT_LOCAL_REPLICATION;
  }
}

#endif
