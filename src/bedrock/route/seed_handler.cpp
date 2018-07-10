#include "hash_ring.hpp"
#include "spdlog/spdlog.h"

// TODO(vikram): shorten some of these variable names for readability
void seed_handler(
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* addr_responder,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unsigned long long duration) {
  logger->info("Received an address request.");
  zmq_util::recv_string(addr_responder);

  communication::Address address;
  address.set_start_time(duration);
  for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end();
       it++) {
    unsigned tier_id = it->first;
    auto hash_ring = &(it->second);
    std::unordered_set<Address> observed_ip;

    for (auto iter = hash_ring->begin(); iter != hash_ring->end(); iter++) {
      if (observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
        communication::Address_Tuple* tp = address.add_tuple();
        tp->set_tier_id(tier_id);
        tp->set_ip(iter->second.get_ip());

        observed_ip.insert(iter->second.get_ip());
      }
    }
  }

  std::string serialized_address;
  address.SerializeToString(&serialized_address);
  zmq_util::send_string(serialized_address, addr_responder);
}
