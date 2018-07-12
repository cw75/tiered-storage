#include "route/routing_handlers.hpp"

void address_handler(
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* key_address_puller,
    SocketCache& pushers, RoutingThread& rt,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, KeyInfo>& placement,
    PendingMap<std::pair<Address, std::string>>& pending_key_request_map,
    unsigned& seed) {

  logger->info("Received key address request.");
  std::string serialized = zmq_util::recv_string(key_address_puller);
  KeyAddressRequest addr_request;
  addr_request.ParseFromString(serialized);

  KeyAddressResponse addr_response;
  addr_response.set_response_id(addr_request.request_id());
  bool succeed;

  int num_servers = 0;
  for (const auto& global_pair : global_hash_ring_map) {
    num_servers += global_pair.second.size();
  }

  bool respond = false;

  if (num_servers == 0) {
    addr_response.set_error(1);
    respond = true;
  } else {  // if there are servers, attempt to return the correct threads
    for (const Key& key : addr_request.keys()) {
      unsigned tier_id = 1;
      ServerThreadSet threads = {};

      while (threads.size() == 0 && tier_id < kMaxTier) {
        threads = get_responsible_threads(
            rt.get_replication_factor_connect_addr(), key, false,
            global_hash_ring_map, local_hash_ring_map, placement, pushers,
            {tier_id}, succeed, seed);

        if (!succeed) {  // this means we don't have the replication factor for
                         // the key
          pending_key_request_map[key].push_back(
              std::pair<Address, std::string>(addr_request.response_address(),
                                              addr_request.request_id()));
          return;
        }

        tier_id++;
      }

      KeyAddressResponse_KeyAddress* tp = addr_response.add_addresses();
      tp->set_key(key);
      respond = true;
      addr_response.set_error(0);

      for (const ServerThread& thread : threads) {
        tp->add_ips(thread.get_request_pulling_connect_addr());
      }
    }

  }

  if (respond) {
    std::string serialized;
    addr_response.SerializeToString(&serialized);

    zmq_util::send_string(serialized, &pushers[addr_request.response_address()]);
  }
}
