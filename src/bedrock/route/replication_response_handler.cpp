#include "route/routing_handlers.hpp"

void replication_response_handler(
    std::shared_ptr<spdlog::logger> logger,
    zmq::socket_t* replication_factor_puller, SocketCache& pushers,
    RoutingThread& rt,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, KeyInfo>& placement,
    PendingMap<std::pair<Address, std::string>>& pending_key_request_map,
    unsigned& seed) {
  std::string serialized_response =
      zmq_util::recv_string(replication_factor_puller);
  communication::Response response;
  response.ParseFromString(serialized_response);

  std::vector<std::string> tokens;
  split(response.tuple(0).key(), '_', tokens);
  Key key = tokens[1];

  if (response.tuple(0).err_number() == 0) {
    communication::Replication_Factor rep_data;
    rep_data.ParseFromString(response.tuple(0).value());

    for (const auto& global : rep_data.global()) {
      placement[key].global_replication_map_[global.tier_id()] =
          global.global_replication();
    }

    for (const auto& local : rep_data.local()) {
      placement[key].local_replication_map_[local.tier_id()] =
          local.local_replication();
    }
  } else if (response.tuple(0).err_number() == 2) {
    logger->info("Retrying rep factor query for key {}.", key);

    auto respond_address = rt.get_replication_factor_connect_addr();
    issue_replication_factor_request(respond_address, key,
                                     global_hash_ring_map[1],
                                     local_hash_ring_map[1], pushers, seed);
  } else {
    init_replication(placement, key);
  }

  if (response.tuple(0).err_number() != 2) {
    // process pending key address requests
    if (pending_key_request_map.find(key) != pending_key_request_map.end()) {
      bool succeed;
      unsigned tier_id = 1;
      ServerThreadSet threads = {};

      while (threads.size() == 0 && tier_id < kMaxTier) {
        threads = get_responsible_threads(
            rt.get_replication_factor_connect_addr(), key, false,
            global_hash_ring_map, local_hash_ring_map, placement, pushers,
            {tier_id}, succeed, seed);

        if (!succeed) {
          logger->error("Missing replication factor for key {}.", key);
          return;
        }

        tier_id++;
      }

      for (const auto& pending_key_req : pending_key_request_map[key]) {
        communication::Key_Response key_res;
        key_res.set_response_id(pending_key_req.second);
        communication::Key_Response_Tuple* tp = key_res.add_tuple();
        tp->set_key(key);

        for (const ServerThread& thread : threads) {
          tp->add_addresses(thread.get_request_pulling_connect_addr());
        }

        // send the key address response
        key_res.set_err_number(0);
        std::string serialized_key_res;
        key_res.SerializeToString(&serialized_key_res);
        zmq_util::send_string(serialized_key_res,
                              &pushers[pending_key_req.first]);
      }

      pending_key_request_map.erase(key);
    }
  }
}
