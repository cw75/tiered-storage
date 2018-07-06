#ifndef __COMMON_H__
#define __COMMON_H__

#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include "communication.pb.h"
#include "zmq/socket_cache.hpp"
#include "zmq/zmq_util.hpp"

using namespace std;
const string METADATA_IDENTIFIER = "BEDROCKMETADATA";

const unsigned METADATA_REPLICATION_FACTOR = 2;
const unsigned METADATA_LOCAL_REPLICATION_FACTOR = 1;

const unsigned VIRTUAL_THREAD_NUM = 3000;

const unsigned MIN_TIER = 1;
const unsigned MAX_TIER = 2;

const unsigned SLO_WORST = 3000;
const unsigned SLO_BEST = 1500;

const unsigned MEM_NODE_CAPACITY = 60000000;
const unsigned EBS_NODE_CAPACITY = 256000000;

// define server base ports
const unsigned SERVER_PORT = 6000;
const unsigned NODE_JOIN_BASE_PORT = 6050;
const unsigned NODE_DEPART_BASE_PORT = 6100;
const unsigned SELF_DEPART_BASE_PORT = 6150;
const unsigned SERVER_REPLICATION_FACTOR_BASE_PORT = 6200;
const unsigned SERVER_REQUEST_PULLING_BASE_PORT = 6250;
const unsigned GOSSIP_BASE_PORT = 6300;
const unsigned SERVER_REPLICATION_FACTOR_CHANGE_BASE_PORT = 6350;

// define routing base ports
const unsigned SEED_BASE_PORT = 6400;
const unsigned ROUTING_NOTIFY_BASE_PORT = 6450;
const unsigned ROUTING_KEY_ADDRESS_BASE_PORT = 6500;
const unsigned ROUTING_REPLICATION_FACTOR_BASE_PORT = 6550;
const unsigned ROUTING_REPLICATION_FACTOR_CHANGE_BASE_PORT = 6600;

// used by monitoring nodes
const unsigned MON_NOTIFY_BASE_PORT = 6650;
const unsigned MON_REQUEST_PULLING_BASE_PORT = 6700;
const unsigned DEPART_DONE_BASE_PORT = 6750;
const unsigned LATENCY_REPORT_BASE_PORT = 6800;

// used by user nodes
const unsigned USER_REQUEST_PULLING_BASE_PORT = 6850;
const unsigned USER_KEY_ADDRESS_BASE_PORT = 6900;
const unsigned COMMAND_BASE_PORT = 6950;

// compile time constants
// extern const string METADATA_IDENTIFIER;
// 
// extern const unsigned METADATA_REPLICATION_FACTOR;
// extern const unsigned METADATA_LOCAL_REPLICATION_FACTOR;
// 
// extern const unsigned VIRTUAL_THREAD_NUM;
// 
// extern const unsigned MIN_TIER;
// extern const unsigned MAX_TIER;
// 
// extern const unsigned MEM_NODE_CAPACITY;
// extern const unsigned EBS_NODE_CAPACITY;
// 
// // define server base ports
// extern const unsigned SERVER_PORT;
// extern const unsigned NODE_JOIN_BASE_PORT;
// extern const unsigned NODE_DEPART_BASE_PORT;
// extern const unsigned SELF_DEPART_BASE_PORT;
// extern const unsigned SERVER_REPLICATION_FACTOR_BASE_PORT;
// extern const unsigned SERVER_REQUEST_PULLING_BASE_PORT;
// extern const unsigned GOSSIP_BASE_PORT;
// extern const unsigned SERVER_REPLICATION_FACTOR_CHANGE_BASE_PORT;
// 
// // define routing base ports
// extern const unsigned SEED_BASE_PORT;
// extern const unsigned ROUTING_NOTIFY_BASE_PORT;
// extern const unsigned ROUTING_KEY_ADDRESS_BASE_PORT;
// extern const unsigned ROUTING_REPLICATION_FACTOR_BASE_PORT;
// extern const unsigned ROUTING_REPLICATION_FACTOR_CHANGE_BASE_PORT;
// 
// // used by monitoring nodes
// extern const unsigned MON_NOTIFY_BASE_PORT;
// extern const unsigned MON_REQUEST_PULLING_BASE_PORT;
// extern const unsigned DEPART_DONE_BASE_PORT;
// extern const unsigned LATENCY_REPORT_BASE_PORT;
// 
// // used by user nodes
// extern const unsigned USER_REQUEST_PULLING_BASE_PORT;
// extern const unsigned USER_KEY_ADDRESS_BASE_PORT;
// extern const unsigned COMMAND_BASE_PORT;

// run-time constants
extern unsigned MEMORY_THREAD_NUM;
extern unsigned EBS_THREAD_NUM;
extern unsigned ROUTING_THREAD_NUM;

extern unsigned DEFAULT_GLOBAL_MEMORY_REPLICATION;
extern unsigned DEFAULT_GLOBAL_EBS_REPLICATION;
extern unsigned DEFAULT_LOCAL_REPLICATION;
extern unsigned MINIMUM_REPLICA_NUMBER;

inline void split(const std::string &s, char delim, std::vector<std::string> &elems) {
  std::stringstream ss(s);
  std::string item;

  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
}

// form the timestamp given a time and a thread id
inline unsigned long long generate_timestamp(unsigned long long time, unsigned tid) {
  unsigned pow = 10;
  while(tid >= pow)
    pow *= 10;
  return time * pow + tid;
}

inline void prepare_get_tuple(communication::Request& req, string key) {
  communication::Request_Tuple* tp = req.add_tuple();
  tp->set_key(key);
}

inline void prepare_put_tuple(communication::Request& req, string key, string value, unsigned long long timestamp) {
  communication::Request_Tuple* tp = req.add_tuple();
  tp->set_key(key);
  tp->set_value(value);
  tp->set_timestamp(timestamp);
}

inline void push_request(communication::Request& req, zmq::socket_t& socket) {
  string serialized_req;
  req.SerializeToString(&serialized_req);
  zmq_util::send_string(serialized_req, &socket);
}

#endif
