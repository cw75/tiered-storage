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
    std::unordered_set<Address> observed_ip;

    for (const auto& hash_pair : hash_ring) {
      std::string thread_ip = hash_pair.second.get_ip();

      if (observed_ip.find(thread_ip) == observed_ip.end()) {
        TierMembership_Tier* tier = membership.add_tiers();
        tier->set_tier_id(tier_id);
        tier->add_ips(thread_ip);

        observed_ip.insert(thread_ip);
      }
    }
  }

  std::string serialized;
  membership.SerializeToString(&serialized);
  zmq_util::send_string(serialized, addr_responder);
}
