#include "monitor/monitoring_utils.hpp"
#include "requests.hpp"

KeyInfo create_new_replication_vector(unsigned gm, unsigned ge, unsigned lm,
                                      unsigned le) {
  KeyInfo vector;
  vector.global_replication_map_[1] = gm;
  vector.global_replication_map_[2] = ge;
  vector.local_replication_map_[1] = lm;
  vector.local_replication_map_[2] = le;

  return vector;
}

void prepare_replication_factor_update(
    const std::string& key,
    std::unordered_map<std::string, communication::Replication_Factor_Request>&
        replication_factor_map,
    std::string server_address,
    std::unordered_map<std::string, KeyInfo>& placement) {
  communication::Replication_Factor_Request_Tuple* tp =
      replication_factor_map[server_address].add_tuple();
  tp->set_key(key);

  for (const auto& rep_pair : placement[key].global_replication_map_) {
    communication::Replication_Factor_Request_Global* g = tp->add_global();
    g->set_tier_id(rep_pair.first);
    g->set_global_replication(rep_pair.second);
  }

  for (const auto& rep_pair : placement[key].local_replication_map_) {
    communication::Replication_Factor_Request_Local* l = tp->add_local();
    l->set_tier_id(rep_pair.first);
    l->set_local_replication(rep_pair.second);
  }
}

// assume the caller has the replication factor for the keys and the requests
// are valid (rep factor <= total number of nodes in a tier)
void change_replication_factor(
    std::unordered_map<std::string, KeyInfo>& requests,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::vector<std::string>& routing_address,
    std::unordered_map<std::string, KeyInfo>& placement, SocketCache& pushers,
    MonitoringThread& mt, zmq::socket_t& response_puller,
    std::shared_ptr<spdlog::logger> logger, unsigned& rid) {
  // used to keep track of the original replication factors for the requested
  // keys
  std::unordered_map<std::string, KeyInfo> orig_placement_info;

  // store the new replication factor synchronously in storage servers
  std::unordered_map<std::string, communication::Request> addr_request_map;

  // form the placement request map
  std::unordered_map<std::string, communication::Replication_Factor_Request>
      replication_factor_map;

  for (const auto& request_pair : requests) {
    std::string key = request_pair.first;
    orig_placement_info[key] = placement[key];

    // update the placement map
    for (const auto& rep_pair : request_pair.second.global_replication_map_) {
      placement[key].global_replication_map_[rep_pair.first] = rep_pair.second;
    }

    for (const auto& rep_pair : request_pair.second.local_replication_map_) {
      placement[key].local_replication_map_[rep_pair.first] = rep_pair.second;
    }

    // prepare data to be stored in the storage tier
    communication::Replication_Factor rep_data;
    for (const auto& rep_pair : placement[key].global_replication_map_) {
      communication::Replication_Factor_Global* g = rep_data.add_global();
      g->set_tier_id(rep_pair.first);
      g->set_global_replication(rep_pair.second);
    }

    for (const auto& rep_pair : placement[key].local_replication_map_) {
      communication::Replication_Factor_Local* l = rep_data.add_local();
      l->set_tier_id(rep_pair.first);
      l->set_local_replication(rep_pair.second);
    }

    std::string rep_key =
        std::string(kMetadataIdentifier) + "_" + key + "_replication";

    std::string serialized_rep_data;
    rep_data.SerializeToString(&serialized_rep_data);
    prepare_metadata_put_request(
        rep_key, serialized_rep_data, global_hash_ring_map[1],
        local_hash_ring_map[1], addr_request_map, mt, rid);
  }

  // send updates to storage nodes
  std::unordered_set<std::string> failed_keys;
  for (const auto& request_pair : addr_request_map) {
    bool succeed;
    auto res = send_request<communication::Request, communication::Response>(
        request_pair.second, pushers[request_pair.first], response_puller,
        succeed);

    if (!succeed) {
      logger->error("Replication factor put timed out!");

      for (const auto& tuple : request_pair.second.tuple()) {
        std::vector<std::string> tokens;
        split(tuple.key(), '_', tokens);

        failed_keys.insert(tokens[1]);
      }
    } else {
      for (const auto& tuple : res.tuple()) {
        if (tuple.err_number() == 2) {
          logger->error(
              "Replication factor put for key {} rejected due to incorrect "
              "address.",
              tuple.key());

          std::vector<std::string> tokens;

          split(tuple.key(), '_', tokens);
          failed_keys.insert(tokens[1]);
        }
      }
    }
  }

  for (const auto& request_pair : requests) {
    std::string key = request_pair.first;

    if (failed_keys.find(key) == failed_keys.end()) {
      for (const unsigned& tier : kAllTierIds) {
        unsigned rep =
            std::max(placement[key].global_replication_map_[tier],
                     orig_placement_info[key].global_replication_map_[tier]);
        ServerThreadSet threads = responsible_global(key, rep, global_hash_ring_map[tier]);

        for (const ServerThread& thread : threads) {
          prepare_replication_factor_update(
              key, replication_factor_map,
              thread.get_replication_factor_change_connect_addr(),
              placement);
        }
      }

      // form placement requests for routing nodes
      for (const std::string& address : routing_address) {
        prepare_replication_factor_update(
            key, replication_factor_map,
            RoutingThread(address, 0)
                .get_replication_factor_change_connect_addr(),
            placement);
      }
    }
  }

  // send placement info update to all relevant nodes
  for (const auto& rep_factor_pair : replication_factor_map) {
    std::string serialized_msg;
    rep_factor_pair.second.SerializeToString(&serialized_msg);
    zmq_util::send_string(serialized_msg, &pushers[rep_factor_pair.first]);
  }

  // restore rep factor for failed keys
  for (const std::string& key : failed_keys) {
    placement[key] = orig_placement_info[key];
  }
}
