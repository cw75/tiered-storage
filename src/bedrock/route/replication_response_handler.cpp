#include "hash_ring.hpp"
#include "spdlog/spdlog.h"

using namespace std;

void replication_response_handler(
    std::shared_ptr<spdlog::logger> logger,
    zmq::socket_t* replication_factor_puller, SocketCache& pushers,
    RoutingThread& rt, unordered_map<unsigned, TierData>& tier_data_map,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    unordered_map<string, KeyInfo>& placement,
    unordered_map<string, pair<chrono::system_clock::time_point,
                               vector<pair<string, string>>>>&
        pending_key_request_map,
    unsigned& seed) {
  string serialized_response = zmq_util::recv_string(replication_factor_puller);
  communication::Response response;
  response.ParseFromString(serialized_response);

  vector<string> tokens;
  split(response.tuple(0).key(), '_', tokens);
  string key = tokens[1];

  if (response.tuple(0).err_number() == 0) {
    communication::Replication_Factor rep_data;
    rep_data.ParseFromString(response.tuple(0).value());

    for (int i = 0; i < rep_data.global_size(); i++) {
      placement[key].global_replication_map_[rep_data.global(i).tier_id()] =
          rep_data.global(i).global_replication();
    }

    for (int i = 0; i < rep_data.local_size(); i++) {
      placement[key].local_replication_map_[rep_data.local(i).tier_id()] =
          rep_data.local(i).local_replication();
    }
  } else if (response.tuple(0).err_number() == 2) {
    logger->info("Retrying rep factor query for key {}.", key);

    auto respond_address = rt.get_replication_factor_connect_addr();
    issue_replication_factor_request(respond_address, key,
                                     global_hash_ring_map[1],
                                     local_hash_ring_map[1], pushers, seed);
  } else {
    for (unsigned i = MIN_TIER; i <= MAX_TIER; i++) {
      placement[key].global_replication_map_[i] =
          tier_data_map[i].default_replication_;
      placement[key].local_replication_map_[i] = DEFAULT_LOCAL_REPLICATION;
    }
  }

  if (response.tuple(0).err_number() != 2) {
    // process pending key address requests
    if (pending_key_request_map.find(key) != pending_key_request_map.end()) {
      bool succeed;
      vector<unsigned> tier_ids;

      // first check memory tier
      tier_ids.push_back(1);
      auto threads = get_responsible_threads(
          rt.get_replication_factor_connect_addr(), key, false,
          global_hash_ring_map, local_hash_ring_map, placement, pushers,
          tier_ids, succeed, seed);

      if (succeed) {
        if (threads.size() == 0) {
          // check ebs tier
          tier_ids.clear();
          tier_ids.push_back(2);
          threads = get_responsible_threads(
              rt.get_replication_factor_connect_addr(), key, false,
              global_hash_ring_map, local_hash_ring_map, placement, pushers,
              tier_ids, succeed, seed);
        }

        for (auto it = pending_key_request_map[key].second.begin();
             it != pending_key_request_map[key].second.end(); it++) {
          communication::Key_Response key_res;
          key_res.set_response_id(it->second);
          communication::Key_Response_Tuple* tp = key_res.add_tuple();
          tp->set_key(key);

          for (auto iter = threads.begin(); iter != threads.end(); iter++) {
            tp->add_addresses(iter->get_request_pulling_connect_addr());
          }

          // send the key address response
          key_res.set_err_number(0);
          string serialized_key_res;
          key_res.SerializeToString(&serialized_key_res);
          zmq_util::send_string(serialized_key_res, &pushers[it->first]);
        }
      } else {
        logger->info("Error: Missing replication factor for key {}.", key);
      }
      pending_key_request_map.erase(key);
    }
  }
}