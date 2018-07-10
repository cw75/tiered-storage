#include "spdlog/spdlog.h"
#include "threads.hpp"

void replication_change_handler(
    std::shared_ptr<spdlog::logger> logger,
    zmq::socket_t* replication_factor_change_puller, SocketCache& pushers,
    std::unordered_map<Key, KeyInfo>& placement, unsigned thread_id,
    Address ip) {
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

  for (int i = 0; i < req.tuple_size(); i++) {
    Key key = req.tuple(i).key();
    // update the replication factor

    for (int j = 0; j < req.tuple(i).global_size(); j++) {
      placement[key].global_replication_map_[req.tuple(i).global(j).tier_id()] =
          req.tuple(i).global(j).global_replication();
    }

    for (int j = 0; j < req.tuple(i).local_size(); j++) {
      placement[key].local_replication_map_[req.tuple(i).local(j).tier_id()] =
          req.tuple(i).local(j).local_replication();
    }
  }
}
