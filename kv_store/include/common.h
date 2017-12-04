#ifndef __COMMON_H__
#define __COMMON_H__

#include <atomic>
#include <string>
#include <boost/functional/hash.hpp>
#include <boost/format.hpp>
#include <boost/crc.hpp>
#include <functional>
#include "consistent_hash_map.hpp"

using namespace std;

// Define global ebs replication factor
#define GLOBAL_EBS_REPLICATION 2

// Define global memory replication factor
#define GLOBAL_MEMORY_REPLICATION 1

// Define the number of proxy worker threads
#define PROXY_THREAD_NUM 3

// Define port offset
#define SERVER_PORT 6560
#define PROXY_CONNECTION_BASE_PORT 6460
#define NOTIFY_PORT 7060
#define PROXY_USER_PORT 7160
#define PROXY_GOSSIP_PORT 7260
#define PROXY_PLACEMENT_PORT 7360
#define PROXY_BENCHMARK_PORT 7460
#define STORAGE_CONSUMPTION_PORT 7460
#define KEY_HOTNESS_PORT 7560
#define REPLICATION_FACTOR_PORT 6360
#define SEED_CONNECTION_PORT 6560
#define CHANGESET_PORT 6560
#define LOCAL_GOSSIP_BASE_PORT 6560
#define DISTRIBUTED_GOSSIP_BASE_PORT 6560
#define LOCAL_REDISTRIBUTE_BASE_PORT 6660
#define LOCAL_DEPART_BASE_PORT 6760
#define LOCAL_DEPART_DONE_PORT 6860
#define LOCAL_KEY_REMOVE_PORT 6960
#define NODE_JOIN_PORT 6660
#define NODE_DEPART_PORT 6760
#define KEY_EXCHANGE_PORT 6860

#define SEED_BIND_ADDR "tcp://*:6560"
#define NOTIFY_BIND_ADDR "tcp://*:7060"
#define NODE_JOIN_BIND_ADDR "tcp://*:6660"
#define NODE_DEPART_BIND_ADDR "tcp://*:6760"
#define KEY_EXCHANGE_BIND_ADDR "tcp://*:6860"
#define LOCAL_DEPART_DONE_ADDR "inproc://6860"
#define PROXY_CONTACT_BIND_ADDR "tcp://*:7160"
#define PROXY_GOSSIP_BIND_ADDR "tcp://*:7260"
#define PROXY_PLACEMENT_BIND_ADDR "tcp://*:7360"
#define STORAGE_CONSUMPTION_BIND_ADDR "tcp://*:7460"
#define KEY_HOTNESS_BIND_ADDR "tcp://*:7560"
#define LOCAL_STORAGE_CONSUMPTION_ADDR "inproc://7460"
#define REPLICATION_FACTOR_BIND_ADDR "tcp://*:6360"
#define CHANGESET_ADDR "inproc://6560"
#define SELF_DEPART_BIND_ADDR "tcp://*:6960"

#define SERVER_IP_FILE "conf/server/server_ip.txt"
#define PROXY_IP_FILE "conf/proxy/proxy_ip.txt"

class node_t {
public:
  node_t() {}
  node_t(string ip, size_t port): ip_(ip), port_(port) {
    id_ = ip + ":" + to_string(port);
  }
  string id_;
  string ip_;
  size_t port_;
};

class master_node_t: public node_t {
public:
  master_node_t() : node_t() {}
  master_node_t(string ip, string tier) : node_t(ip, SERVER_PORT) {
    tier_ = tier;
    seed_connection_connect_addr_ = "tcp://" + ip + ":" + to_string(SEED_CONNECTION_PORT);
    node_join_connect_addr_ = "tcp://" + ip + ":" + to_string(NODE_JOIN_PORT);
    node_depart_connect_addr_ = "tcp://" + ip + ":" + to_string(NODE_DEPART_PORT);
    key_exchange_connect_addr_ = "tcp://" + ip + ":" + to_string(KEY_EXCHANGE_PORT);
    replication_factor_connect_addr_ = "tcp://" + ip + ":" + to_string(REPLICATION_FACTOR_PORT);
  }

  string tier_;
  string seed_connection_connect_addr_;
  string node_join_connect_addr_;
  string node_depart_connect_addr_;
  string key_exchange_connect_addr_;
  string replication_factor_connect_addr_;
};

class worker_node_t: public node_t {
public:
  worker_node_t() : node_t() {}
  worker_node_t(string ip, int tid) : node_t(ip, SERVER_PORT + tid) {
    proxy_connection_connect_addr_ = "tcp://" + ip + ":" + to_string(tid + PROXY_CONNECTION_BASE_PORT);
    proxy_connection_bind_addr_ = "tcp://*:" + to_string(tid + PROXY_CONNECTION_BASE_PORT);

    distributed_gossip_connect_addr_ = "tcp://" + ip + ":" + to_string(tid + DISTRIBUTED_GOSSIP_BASE_PORT);
    distributed_gossip_bind_addr_ = "tcp://*:" + to_string(tid + DISTRIBUTED_GOSSIP_BASE_PORT);

    local_gossip_addr_ = "inproc://" + to_string(tid + LOCAL_GOSSIP_BASE_PORT);
    local_redistribute_addr_ = "inproc://" + to_string(tid + LOCAL_REDISTRIBUTE_BASE_PORT);
    local_depart_addr_ = "inproc://" + to_string(tid + LOCAL_DEPART_BASE_PORT);
    local_key_remove_addr_ = "inproc://" + to_string(tid + LOCAL_KEY_REMOVE_PORT);
  }
  string proxy_connection_connect_addr_;
  string proxy_connection_bind_addr_;
  string local_gossip_addr_;
  string distributed_gossip_connect_addr_;
  string distributed_gossip_bind_addr_;
  string local_redistribute_addr_;
  string local_depart_addr_;
  string local_key_remove_addr_;
};

class proxy_node_t {
public:
  proxy_node_t() {}
  proxy_node_t(string ip): ip_(ip) {
    notify_connect_addr_ = "tcp://" + ip + ":" + to_string(NOTIFY_PORT);
    replication_factor_connect_addr_ = "tcp://" + ip + ":" + to_string(REPLICATION_FACTOR_PORT);
  }
  string ip_;
  string notify_connect_addr_;
  string replication_factor_connect_addr_;
};

class proxy_worker_thread_t {
public:
  proxy_worker_thread_t() {}
  proxy_worker_thread_t(string ip, int tid): ip_(ip) {
    user_request_connect_addr_ = "tcp://" + ip + ":" + to_string(tid + PROXY_USER_PORT);
    user_request_bind_addr_ = "tcp://*:" + to_string(tid + PROXY_USER_PORT);
    proxy_gossip_connect_addr_ = "tcp://" + ip + ":" + to_string(tid + PROXY_GOSSIP_PORT);
    proxy_gossip_bind_addr_ = "tcp://*:" + to_string(tid + PROXY_GOSSIP_PORT);
    banchmark_connect_addr_ = "tcp://" + ip + ":" + to_string(tid + PROXY_BENCHMARK_PORT);
    banchmark_bind_addr_ = "tcp://*:" + to_string(tid + PROXY_BENCHMARK_PORT);
  }
  string ip_;
  string user_request_connect_addr_;
  string user_request_bind_addr_;
  string proxy_gossip_connect_addr_;
  string proxy_gossip_bind_addr_;
  string banchmark_connect_addr_;
  string banchmark_bind_addr_;
};

class monitoring_node_t {
public:
  monitoring_node_t() {}
  monitoring_node_t(string ip): ip_(ip) {
    notify_connect_addr_ = "tcp://" + ip + ":" + to_string(NOTIFY_PORT);
    replication_factor_connect_addr_ = "tcp://" + ip + ":" + to_string(REPLICATION_FACTOR_PORT);
    storage_consumption_connect_addr_ = "tcp://" + ip + ":" + to_string(STORAGE_CONSUMPTION_PORT);
    key_hotness_connect_addr_ = "tcp://" + ip + ":" + to_string(KEY_HOTNESS_PORT);
  }
  string ip_;
  string notify_connect_addr_;
  string replication_factor_connect_addr_;
  string storage_consumption_connect_addr_;
  string key_hotness_connect_addr_;
};

bool operator<(const node_t& l, const node_t& r) {
  if (l.id_.compare(r.id_) == 0) {
    return false;
  } else {
    return true;
  }
}

bool operator==(const node_t& l, const node_t& r) {
  if (l.id_.compare(r.id_) == 0) {
    return true;
  } else {
    return false;
  }
}

struct node_hash {
  std::size_t operator () (const node_t &n) const {
    return std::hash<string>{}(n.id_);
  }
};

// represents the replication state for each key (used in proxy and memory tier)
struct key_info {
  key_info() : global_memory_replication_(1), global_ebs_replication_(2) {}
  key_info(int gmr, int ger)
    : global_memory_replication_(gmr), global_ebs_replication_(ger) {}
  int global_memory_replication_;
  int global_ebs_replication_;
};

// represents the replication state for each key (used in proxy and memory tier)
struct shared_key_info {
  shared_key_info() : global_memory_replication_(1), global_ebs_replication_(2) {}
  shared_key_info(int gmr, int ger)
    : global_memory_replication_(gmr), global_ebs_replication_(ger) {}
  atomic<int> global_memory_replication_;
  atomic<int> global_ebs_replication_;
};

struct crc32_hasher {
  uint32_t operator()(const node_t& node) {
    boost::crc_32_type ret;
    ret.process_bytes(node.id_.c_str(), node.id_.size());
    return ret.checksum();
  }
  uint32_t operator()(const string& key) {
    boost::crc_32_type ret;
    ret.process_bytes(key.c_str(), key.size());
    return ret.checksum();
  }
  typedef uint32_t result_type;
};

struct ebs_hasher {
  hash<string>::result_type operator()(const node_t& node) {
    return hash<string>{}(node.id_);
  }
  hash<string>::result_type operator()(const string& key) {
    return hash<string>{}(key);
  }
  typedef hash<string>::result_type result_type;
};

void split(const string &s, char delim, vector<string> &elems) {
  stringstream ss(s);
  string item;
  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
}

size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
  ((std::string*)userp)->append((char*)contents, size * nmemb);
  return size * nmemb;
}

string get_ip(string node_type) {
  string server_ip;
  ifstream address;

  if (node_type == "server") {
    address.open(SERVER_IP_FILE);
  } else if (node_type == "proxy") {
    address.open(PROXY_IP_FILE);
  }
  std::getline(address, server_ip);
  address.close();

  return server_ip;
}

typedef consistent_hash_map<master_node_t,crc32_hasher> global_hash_t;

#endif
