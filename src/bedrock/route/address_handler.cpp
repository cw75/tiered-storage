#include "hash_ring.hpp"
#include "spdlog/spdlog.h"

void address_handler(
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* key_address_puller,
    SocketCache& pushers, RoutingThread& rt,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<std::string, KeyInfo>& placement,
    std::unordered_map<
        std::string,
        std::pair<std::chrono::system_clock::time_point,
                  std::vector<std::pair<std::string, std::string>>>>&
        pending_key_request_map,
    unsigned& seed) {
  logger->info("Received key address request.");
  std::string serialized_key_req = zmq_util::recv_string(key_address_puller);
  communication::Key_Request key_req;
  key_req.ParseFromString(serialized_key_req);

  communication::Key_Response key_res;
  key_res.set_response_id(key_req.request_id());
  bool succeed;

  int num_servers = 0;
  for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end();
       ++it) {
    num_servers += it->second.size();
  }

  if (num_servers == 0) {
    key_res.set_err_number(1);

    std::string serialized_key_res;
    key_res.SerializeToString(&serialized_key_res);

    zmq_util::send_string(serialized_key_res,
                          &pushers[key_req.respond_address()]);
  } else {  // if there are servers, attempt to return the correct threads
    for (int i = 0; i < key_req.keys_size(); i++) {
      unsigned tier_id = 1;
      std::unordered_set<ServerThread, ThreadHash> threads = {};
      std::string key = key_req.keys(i);

      while (threads.size() == 0 && tier_id < kMaxTier) {
        threads = get_responsible_threads(
            rt.get_replication_factor_connect_addr(), key, false,
            global_hash_ring_map, local_hash_ring_map, placement, pushers,
            { tier_id }, succeed, seed);

        if (!succeed) { // this means we don't have the replication factor for the key
          if (pending_key_request_map.find(key) ==
              pending_key_request_map.end()) {
            pending_key_request_map[key].first = std::chrono::system_clock::now();
          }

          pending_key_request_map[key].second.push_back(
              std::pair<std::string, std::string>(key_req.respond_address(),
                key_req.request_id()));
          return;
        }

        tier_id++;
      }

      communication::Key_Response_Tuple* tp = key_res.add_tuple();
      tp->set_key(key);

      for (auto it = threads.begin(); it != threads.end(); it++) {
        tp->add_addresses(it->get_request_pulling_connect_addr());
      }
    }

    if (key_res.tuple_size() > 0) {
      key_res.set_err_number(0);

      std::string serialized_key_res;
      key_res.SerializeToString(&serialized_key_res);

      zmq_util::send_string(serialized_key_res,
                            &pushers[key_req.respond_address()]);
    }
  }
}
