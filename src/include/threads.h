#ifndef __THREADS_H__
#define __THREADS_H__

#include "utils/consistent_hash_map.hpp"
#include "common.h"
#include "metadata.h"

using namespace std;


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
    return "tcp://" + ip_ + ":" + to_string(tid_ + REQUEST_PULLING_BASE_PORT);
  }
  string get_request_pulling_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + REQUEST_PULLING_BASE_PORT);
  }
  string get_replication_factor_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + REPLICATION_FACTOR_BASE_PORT);
  }
  string get_replication_factor_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + REPLICATION_FACTOR_BASE_PORT);
  }
  string get_gossip_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + GOSSIP_BASE_PORT);
  }
  string get_gossip_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + GOSSIP_BASE_PORT);
  }
  string get_replication_factor_change_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + REPLICATION_FACTOR_CHANGE_BASE_PORT);
  }
  string get_replication_factor_change_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + REPLICATION_FACTOR_CHANGE_BASE_PORT);
  }
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
    return "tcp://" + ip_ + ":" + to_string(tid_ + NOTIFY_BASE_PORT);
  }

  string get_notify_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + NOTIFY_BASE_PORT);
  }

  string get_key_address_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + KEY_ADDRESS_BASE_PORT);
  }

  string get_key_address_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + KEY_ADDRESS_BASE_PORT);
  }

  string get_replication_factor_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + REPLICATION_FACTOR_BASE_PORT);
  }

  string get_replication_factor_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + REPLICATION_FACTOR_BASE_PORT);
  }

  string get_replication_factor_change_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + REPLICATION_FACTOR_CHANGE_BASE_PORT);
  }

  string get_replication_factor_change_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + REPLICATION_FACTOR_CHANGE_BASE_PORT);
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
    return "tcp://" + ip_ + ":" + to_string(NOTIFY_BASE_PORT);
  }

  string get_notify_bind_addr() const {
    return "tcp://*:" + to_string(NOTIFY_BASE_PORT);
  }

  string get_request_pulling_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(REQUEST_PULLING_BASE_PORT);
  }

  string get_request_pulling_bind_addr() const {
    return "tcp://*:" + to_string(REQUEST_PULLING_BASE_PORT);
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
    return "tcp://" + ip_ + ":" + to_string(tid_ + REQUEST_PULLING_BASE_PORT);
  }

  string get_request_pulling_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + REQUEST_PULLING_BASE_PORT);
  }

  string get_key_address_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + KEY_ADDRESS_BASE_PORT);
  }

  string get_key_address_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + KEY_ADDRESS_BASE_PORT);
  }
};

#endif