#include <fstream>
#include <string>

#include "common.hpp"
#include "hash_ring.hpp"
#include "kvs/kvs_handlers.hpp"
#include "kvs/rc_pair_lattice.hpp"
#include "spdlog/spdlog.h"
#include "zmq/socket_cache.hpp"

void node_depart_handler(
    unsigned int thread_num, unsigned thread_id, string ip,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* depart_puller,
    SocketCache& pushers) {
  string message = zmq_util::recv_string(depart_puller);
  vector<string> v;
  split(message, ':', v);

  unsigned tier = stoi(v[0]);
  string departing_server_ip = v[1];
  logger->info("Received departure for node {} on tier {}.",
               departing_server_ip, tier);

  // update hash ring
  remove_from_hash_ring<GlobalHashRing>(global_hash_ring_map[tier],
                                        departing_server_ip, 0);

  if (thread_id == 0) {
    // tell all worker threads about the node departure
    for (unsigned tid = 1; tid < thread_num; tid++) {
      zmq_util::send_string(
          message,
          &pushers[ServerThread(ip, tid).get_node_depart_connect_addr()]);
    }

    for (auto it = global_hash_ring_map.begin();
         it != global_hash_ring_map.end(); it++) {
      logger->info("Hash ring for tier {} size is {}.", to_string(it->first),
                   to_string(it->second.size()));
    }
  }
}
