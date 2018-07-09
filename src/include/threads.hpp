#ifndef __THREADS_H__
#define __THREADS_H__

#include "common.hpp"
#include "metadata.hpp"
#include "utils/consistent_hash_map.hpp"

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
    return ip_ + ":" + std::to_string(SERVER_PORT + tid_);
  }
  std::string get_virtual_id() const {
    return ip_ + ":" + std::to_string(SERVER_PORT + tid_) + "_" +
           std::to_string(virtual_num_);
  }
  std::string get_node_join_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + NODE_JOIN_BASE_PORT);
  }
  std::string get_node_join_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + NODE_JOIN_BASE_PORT);
  }
  std::string get_node_depart_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + NODE_DEPART_BASE_PORT);
  }
  std::string get_node_depart_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + NODE_DEPART_BASE_PORT);
  }
  std::string get_self_depart_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + SELF_DEPART_BASE_PORT);
  }
  std::string get_self_depart_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + SELF_DEPART_BASE_PORT);
  }
  std::string get_request_pulling_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + SERVER_REQUEST_PULLING_BASE_PORT);
  }
  std::string get_request_pulling_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + SERVER_REQUEST_PULLING_BASE_PORT);
  }
  std::string get_replication_factor_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + SERVER_REPLICATION_FACTOR_BASE_PORT);
  }
  std::string get_replication_factor_bind_addr() const {
    return "tcp://*:" +
           std::to_string(tid_ + SERVER_REPLICATION_FACTOR_BASE_PORT);
  }
  std::string get_gossip_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + GOSSIP_BASE_PORT);
  }
  std::string get_gossip_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + GOSSIP_BASE_PORT);
  }
  std::string get_replication_factor_change_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + SERVER_REPLICATION_FACTOR_CHANGE_BASE_PORT);
  }
  std::string get_replication_factor_change_bind_addr() const {
    return "tcp://*:" +
           std::to_string(tid_ + SERVER_REPLICATION_FACTOR_CHANGE_BASE_PORT);
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
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + SEED_BASE_PORT);
  }

  std::string get_seed_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + SEED_BASE_PORT);
  }

  std::string get_notify_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + ROUTING_NOTIFY_BASE_PORT);
  }

  std::string get_notify_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + ROUTING_NOTIFY_BASE_PORT);
  }

  std::string get_key_address_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + ROUTING_KEY_ADDRESS_BASE_PORT);
  }

  std::string get_key_address_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + ROUTING_KEY_ADDRESS_BASE_PORT);
  }

  std::string get_replication_factor_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + ROUTING_REPLICATION_FACTOR_BASE_PORT);
  }

  std::string get_replication_factor_bind_addr() const {
    return "tcp://*:" +
           std::to_string(tid_ + ROUTING_REPLICATION_FACTOR_BASE_PORT);
  }

  std::string get_replication_factor_change_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + ROUTING_REPLICATION_FACTOR_CHANGE_BASE_PORT);
  }

  std::string get_replication_factor_change_bind_addr() const {
    return "tcp://*:" +
           std::to_string(tid_ + ROUTING_REPLICATION_FACTOR_CHANGE_BASE_PORT);
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
    return "tcp://" + ip_ + ":" + std::to_string(MON_NOTIFY_BASE_PORT);
  }

  std::string get_notify_bind_addr() const {
    return "tcp://*:" + std::to_string(MON_NOTIFY_BASE_PORT);
  }

  std::string get_request_pulling_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(MON_REQUEST_PULLING_BASE_PORT);
  }

  std::string get_request_pulling_bind_addr() const {
    return "tcp://*:" + std::to_string(MON_REQUEST_PULLING_BASE_PORT);
  }

  std::string get_depart_done_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(DEPART_DONE_BASE_PORT);
  }

  std::string get_depart_done_bind_addr() const {
    return "tcp://*:" + std::to_string(DEPART_DONE_BASE_PORT);
  }

  std::string get_latency_report_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(LATENCY_REPORT_BASE_PORT);
  }

  std::string get_latency_report_bind_addr() const {
    return "tcp://*:" + std::to_string(LATENCY_REPORT_BASE_PORT);
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
           std::to_string(tid_ + USER_REQUEST_PULLING_BASE_PORT);
  }

  std::string get_request_pulling_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + USER_REQUEST_PULLING_BASE_PORT);
  }

  std::string get_key_address_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + USER_KEY_ADDRESS_BASE_PORT);
  }

  std::string get_key_address_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + USER_KEY_ADDRESS_BASE_PORT);
  }
};

#endif
