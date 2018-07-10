#ifndef __COMMON_H__
#define __COMMON_H__

#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "communication.pb.h"
#include "types.hpp"
#include "zmq/socket_cache.hpp"
#include "zmq/zmq_util.hpp"

const std::string kMetadataIdentifier = "BEDROCKMETADATA";

const unsigned kMetadataReplicationFactor = 2;
const unsigned kMetadataLocalReplicationFactor = 1;

const unsigned kVirtualThreadNum = 3000;

const unsigned kMinTier = 1;
const unsigned kMaxTier = 2;
const std::vector<unsigned> kAllTierIds = { 1, 2 };

const unsigned kSloWorst = 3000;
const unsigned SLO_BEST = 1500;

const unsigned kMemoryNodeCapacity = 60000000;
const unsigned kEbsNodeCapacity = 256000000;

// define server base ports
const unsigned kNodeJoinBasePort = 6000;
const unsigned kNodeDepartBasePort = 6050;
const unsigned kSelfDepartBasePort = 6100;
const unsigned kServerReplicationFactorBasePort = 6150;
const unsigned kServerRequestPullingBasePort = 6200;
const unsigned kGossipBasePort = 6250;
const unsigned kServerReplicationFactorChangeBasePort = 6300;

// define routing base ports
const unsigned kSeedBasePort = 6350;
const unsigned kRoutingNotifyBasePort = 6400;
const unsigned kRoutingKeyAddressBasePort = 6450;
const unsigned kRoutingReplicationFactorBasePort = 6500;
const unsigned kRoutingReplicationFactorChangeBasePort = 6550;

// used by monitoring nodes
const unsigned kMonitoringNotifyBasePort = 6600;
const unsigned kMonitoringRequestPullingBasePort = 6650;
const unsigned kDepartDoneBasePort = 6700;
const unsigned kLatencyReportBasePort = 6750;

// used by user nodes
const unsigned kUserRequestBasePort = 6800;
const unsigned kUserKeyAddressBasePort = 6850;
const unsigned kBenchmarkCommandBasePort = 6900;

// run-time constants
extern unsigned kSelfTierId;
extern std::vector<unsigned> kSelfTierIdVector;

extern unsigned kMemoryThreadCount;
extern unsigned kEbsThreadCount;
extern unsigned kRoutingThreadCount;

extern unsigned kDefaultGlobalMemoryReplication;
extern unsigned kDefaultGlobalEbsReplication;
extern unsigned kDefaultLocalReplication;
extern unsigned kMinimumReplicaNumber;

inline void split(const std::string& s, char delim,
                  std::vector<std::string>& elems) {
  std::stringstream ss(s);
  std::string item;

  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
}

// form the timestamp given a time and a thread id
inline unsigned long long generate_timestamp(unsigned long long time,
                                             unsigned tid) {
  unsigned pow = 10;
  while (tid >= pow) pow *= 10;
  return time * pow + tid;
}

inline void prepare_get_tuple(communication::Request& req, Key key) {
  communication::Request_Tuple* tp = req.add_tuple();
  tp->set_key(key);
}

inline void prepare_put_tuple(communication::Request& req, Key key,
                              std::string value, unsigned long long timestamp) {
  communication::Request_Tuple* tp = req.add_tuple();
  tp->set_key(key);
  tp->set_value(value);
  tp->set_timestamp(timestamp);
}

inline void push_request(communication::Request& req, zmq::socket_t& socket) {
  std::string serialized_req;
  req.SerializeToString(&serialized_req);
  zmq_util::send_string(serialized_req, &socket);
}

#endif
