#include "route/routing_handlers.hpp"

void replication_change_handler(std::shared_ptr<spdlog::logger> logger,
                                zmq::socket_t* replication_factor_change_puller,
                                SocketCache& pushers,
                                std::unordered_map<Key, KeyInfo>& placement,
                                unsigned thread_id, Address ip) {
  logger->info("Received a replication factor change.");
  std::string update_str =
      zmq_util::recv_string(replication_factor_change_puller);

  if (thread_id == 0) {
    // tell all worker threads about the replication factor change
    for (unsigned tid = 1; tid < kRoutingThreadCount; tid++) {
      zmq_util::send_string(
          update_str,
          &pushers[RoutingThread(ip, tid)
                       .get_replication_factor_change_connect_addr()]);
    }
  }

  ReplicationFactorUpdate update;
  update.ParseFromString(update_str);

  for (const auto& key_rep : update.key_reps()) {
    Key key = key_rep.key();
    // update the replication factor

    for (const Replication& global : key_rep.global()) {
      placement[key].global_replication_map_[global.tier_id()] =
          global.replication_factor();
    }

    for (const Replication& local : key_rep.local()) {
      placement[key].local_replication_map_[local.tier_id()] =
          local.replication_factor();
    }
  }
}
