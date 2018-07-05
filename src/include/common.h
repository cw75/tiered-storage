#ifndef __COMMON_H__
#define __COMMON_H__

#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include "communication.pb.h"
#include "zmq/socket_cache.h"
#include "zmq/zmq_util.h"

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
// initialized in common.cpp
extern unsigned MEM_NODE_CAPACITY;
extern unsigned EBS_NODE_CAPACITY;

// value size in KB
#define VALUE_SIZE 256

// define base ports
#define SERVER_PORT 6560
#define NODE_JOIN_BASE_PORT 6660
#define NODE_DEPART_BASE_PORT 6760
#define SELF_DEPART_BASE_PORT 6860
#define REPLICATION_FACTOR_BASE_PORT 6960
#define REQUEST_PULLING_BASE_PORT 6460
#define GOSSIP_BASE_PORT 7060
#define REPLICATION_FACTOR_CHANGE_BASE_PORT 7160

// used by proxies
#define SEED_BASE_PORT 6560
#define NOTIFY_BASE_PORT 6660
#define KEY_ADDRESS_BASE_PORT 6760

// used by monitoring nodes
#define DEPART_DONE_BASE_PORT 6760
#define LATENCY_REPORT_BASE_PORT 6860

// used by benchmark threads
#define COMMAND_BASE_PORT 6560

void split(const std::string &s, char delim, std::vector<std::string> &elems);
unsigned long long generate_timestamp(unsigned long long time, unsigned tid);

void prepare_get_tuple(communication::Request& req, std::string key);

void prepare_put_tuple(communication::Request& req, std::string key, 
                       std::string value, unsigned long long timestamp);

void push_request(communication::Request& req, zmq::socket_t& socket);

bool is_metadata(std::string key);

#endif
