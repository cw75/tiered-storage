#ifndef __COMMON_H__
#define __COMMON_H__

#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include "communication.pb.h"
#include "zmq/socket_cache.hpp"
#include "zmq/zmq_util.hpp"

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
#define MEM_NODE_CAPACITY 60000000
#define EBS_NODE_CAPACITY 256000000

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

using namespace std;

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
