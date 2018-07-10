#include "spdlog/spdlog.h"
#include "threads.hpp"

void replication_change_handler(
    std::shared_ptr<spdlog::logger> logger,
    zmq::socket_t* replication_factor_change_puller, SocketCache& pushers,
    std::unordered_map<std::string, KeyInfo>& placement, unsigned thread_id,
    std::string ip) {
  logger->info("Received a replication factor change.");
  std::string serialized_req =
      zmq_util::recv_string(replication_factor_change_puller);

  if (thread_id == 0) {
    // tell all worker threads about the replication factor change
    for (unsigned tid = 1; tid < kRoutingThreadCount; tid++) {
      zmq_util::send_string(
          serialized_req,
          &pushers[RoutingThread(ip, tid)
                       .get_replication_factor_change_connect_addr()]);
    }
  }

  communication::Replication_Factor_Request req;
  req.ParseFromString(serialized_req);

  for (const auto& tuple : req.tuple()) {
    std::string key = tuple.key();
    // update the replication factor

    for (const auto& global : tuple.global()) {
      placement[key].global_replication_map_[global.tier_id()] =
          global.global_replication();
    }

    for (const auto& local: tuple.local()) {
      placement[key].local_replication_map_[local.tier_id()] =
          local.local_replication();
    }
  }
}
