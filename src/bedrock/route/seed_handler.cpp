#include "route/routing_handlers.hpp"

void seed_handler(
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* addr_responder,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unsigned long long duration) {
  logger->info("Received an address request.");
  zmq_util::recv_string(addr_responder);

  TierMembership membership;
  membership.set_start_time(duration);

  for (const auto& global_pair : global_hash_ring_map) {
    unsigned tier_id = global_pair.first;
    auto hash_ring = global_pair.second;

    for (const ServerThread& st : hash_ring.get_unique_servers()) {
        TierMembership_Tier* tier = membership.add_tiers();
        tier->set_tier_id(tier_id);
        tier->add_ips(st.get_ip());
    }
  }

  std::string serialized;
  membership.SerializeToString(&serialized);
  zmq_util::send_string(serialized, addr_responder);
}
