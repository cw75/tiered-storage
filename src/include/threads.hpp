#ifndef __THREADS_H__
#define __THREADS_H__

#include "common.hpp"
#include "metadata.hpp"

// server thread
class ServerThread {
  std::string ip_;
  unsigned tid_;
  unsigned virtual_num_;

 public:
  ServerThread() {}
  ServerThread(std::string ip, unsigned tid) : ip_(ip), tid_(tid) {}
  ServerThread(std::string ip, unsigned tid, unsigned virtual_num) :
      ip_(ip),
      tid_(tid),
      virtual_num_(virtual_num) {}

  std::string get_ip() const { return ip_; }
  unsigned get_tid() const { return tid_; }
  unsigned get_virtual_num() const { return virtual_num_; }
  std::string get_id() const {
    return ip_ + ":" + std::to_string(tid_);
  }
  std::string get_virtual_id() const {
    return ip_ + ":" + std::to_string(tid_) + "_" +
           std::to_string(virtual_num_);
  }
  std::string get_node_join_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + kNodeJoinBasePort);
  }
  std::string get_node_join_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kNodeJoinBasePort);
  }
  std::string get_node_depart_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + kNodeDepartBasePort);
  }
  std::string get_node_depart_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kNodeDepartBasePort);
  }
  std::string get_self_depart_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + kSelfDepartBasePort);
  }
  std::string get_self_depart_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kSelfDepartBasePort);
  }
  std::string get_request_pulling_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kServerRequestPullingBasePort);
  }
  std::string get_request_pulling_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kServerRequestPullingBasePort);
  }
  std::string get_replication_factor_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kServerReplicationFactorBasePort);
  }
  std::string get_replication_factor_bind_addr() const {
    return "tcp://*:" +
           std::to_string(tid_ + kServerReplicationFactorBasePort);
  }
  std::string get_gossip_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + kGossipBasePort);
  }
  std::string get_gossip_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kGossipBasePort);
  }
  std::string get_replication_factor_change_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kServerReplicationFactorChangeBasePort);
  }
  std::string get_replication_factor_change_bind_addr() const {
    return "tcp://*:" +
           std::to_string(tid_ + kServerReplicationFactorChangeBasePort);
  }
};

inline bool operator==(const ServerThread& l, const ServerThread& r) {
  if (l.get_id().compare(r.get_id()) == 0) {
    return true;
  } else {
    return false;
  }
}

// routing thread
class RoutingThread {
  std::string ip_;
  unsigned tid_;

 public:
  RoutingThread() {}
  RoutingThread(std::string ip, unsigned tid) : ip_(ip), tid_(tid) {}

  std::string get_ip() const { return ip_; }

  unsigned get_tid() const { return tid_; }

  std::string get_seed_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + kSeedBasePort);
  }

  std::string get_seed_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kSeedBasePort);
  }

  std::string get_notify_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kRoutingNotifyBasePort);
  }

  std::string get_notify_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kRoutingNotifyBasePort);
  }

  std::string get_key_address_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kRoutingKeyAddressBasePort);
  }

  std::string get_key_address_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kRoutingKeyAddressBasePort);
  }

  std::string get_replication_factor_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kRoutingReplicationFactorBasePort);
  }

  std::string get_replication_factor_bind_addr() const {
    return "tcp://*:" +
           std::to_string(tid_ + kRoutingReplicationFactorBasePort);
  }

  std::string get_replication_factor_change_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kRoutingReplicationFactorChangeBasePort);
  }

  std::string get_replication_factor_change_bind_addr() const {
    return "tcp://*:" +
           std::to_string(tid_ + kRoutingReplicationFactorChangeBasePort);
  }
};

// monitoring thread
class MonitoringThread {
  std::string ip_;

 public:
  MonitoringThread() {}
  MonitoringThread(std::string ip) : ip_(ip) {}

  std::string get_ip() const { return ip_; }

  std::string get_notify_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(kMonitoringNotifyBasePort);
  }

  std::string get_notify_bind_addr() const {
    return "tcp://*:" + std::to_string(kMonitoringNotifyBasePort);
  }

  std::string get_request_pulling_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(kMonitoringRequestPullingBasePort);
  }

  std::string get_request_pulling_bind_addr() const {
    return "tcp://*:" + std::to_string(kMonitoringRequestPullingBasePort);
  }

  std::string get_depart_done_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(kDepartDoneBasePort);
  }

  std::string get_depart_done_bind_addr() const {
    return "tcp://*:" + std::to_string(kDepartDoneBasePort);
  }

  std::string get_latency_report_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(kLatencyReportBasePort);
  }

  std::string get_latency_report_bind_addr() const {
    return "tcp://*:" + std::to_string(kLatencyReportBasePort);
  }
};

class UserThread {
  std::string ip_;
  unsigned tid_;

 public:
  UserThread() {}
  UserThread(std::string ip, unsigned tid) : ip_(ip), tid_(tid) {}

  std::string get_ip() const { return ip_; }

  unsigned get_tid() const { return tid_; }

  std::string get_request_pulling_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kUserRequestBasePort);
  }

  std::string get_request_pulling_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kUserRequestBasePort);
  }

  std::string get_key_address_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kUserKeyAddressBasePort);
  }

  std::string get_key_address_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kUserKeyAddressBasePort);
  }
};

#endif
