#include "spdlog/spdlog.h"
#include "hash_ring.hpp"
#include "requests.hpp"
#include "monitor/monitoring_utils.hpp"

using namespace std;

KeyInfo
create_new_replication_vector(unsigned gm, unsigned ge, unsigned lm, unsigned le) {
  KeyInfo rep_vector;
  rep_vector.global_replication_map_[1] = gm;
  rep_vector.global_replication_map_[2] = ge;
  rep_vector.local_replication_map_[1] = lm;
  rep_vector.local_replication_map_[2] = le;

  return rep_vector;
}

void
prepare_replication_factor_update(const string& key,
                                  unordered_map<string, communication::Replication_Factor_Request>& replication_factor_map,
                                  string server_address,
                                  unordered_map<string, KeyInfo>& placement
                                  ) {
  communication::Replication_Factor_Request_Tuple* tp = replication_factor_map[server_address].add_tuple();
  tp->set_key(key);

  for (auto iter = placement[key].global_replication_map_.begin(); iter != placement[key].global_replication_map_.end(); iter++) {
    communication::Replication_Factor_Request_Global* g = tp->add_global();
    g->set_tier_id(iter->first);
    g->set_global_replication(iter->second);
  }

  for (auto iter = placement[key].local_replication_map_.begin(); iter != placement[key].local_replication_map_.end(); iter++) {
    communication::Replication_Factor_Request_Local* l = tp->add_local();
    l->set_tier_id(iter->first);
    l->set_local_replication(iter->second);
  }
}

// assume the caller has the replication factor for the keys and the requests are valid
// (rep factor <= total number of nodes in a tier)
void
change_replication_factor(unordered_map<string, KeyInfo>& requests,
                          unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
                          unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
                          vector<string>& routing_address,
                          unordered_map<string, KeyInfo>& placement,
                          SocketCache& pushers,
                          MonitoringThread& mt,
                          zmq::socket_t& response_puller,
                          shared_ptr<spdlog::logger> logger,
                          unsigned& rid
                          ) {
  // used to keep track of the original replication factors for the requested keys
  unordered_map<string, KeyInfo> orig_placement_info;

  // store the new replication factor synchronously in storage servers
  unordered_map<string, communication::Request> addr_request_map;

  // form the placement request map
  unordered_map<string, communication::Replication_Factor_Request> replication_factor_map;

  for (auto it = requests.begin(); it != requests.end(); it++) {
    string key = it->first;
    orig_placement_info[key] = placement[key];

    // update the placement map
    for (auto iter = it->second.global_replication_map_.begin(); iter != it->second.global_replication_map_.end(); iter++) {
      placement[key].global_replication_map_[iter->first] = iter->second;
    }

    for (auto iter = it->second.local_replication_map_.begin(); iter != it->second.local_replication_map_.end(); iter++) {
      placement[key].local_replication_map_[iter->first] = iter->second;
    }

    // prepare data to be stored in the storage tier
    communication::Replication_Factor rep_data;
    for (auto iter = placement[key].global_replication_map_.begin(); iter != placement[key].global_replication_map_.end(); iter++) {
      communication::Replication_Factor_Global* g = rep_data.add_global();
      g->set_tier_id(iter->first);
      g->set_global_replication(iter->second);
    }

    for (auto iter = placement[key].local_replication_map_.begin(); iter != placement[key].local_replication_map_.end(); iter++) {
      communication::Replication_Factor_Local* l = rep_data.add_local();
      l->set_tier_id(iter->first);
      l->set_local_replication(iter->second);
    }

    string rep_key = string(METADATA_IDENTIFIER) + "_" + key + "_replication";

    string serialized_rep_data;
    rep_data.SerializeToString(&serialized_rep_data);
    prepare_metadata_put_request(rep_key, serialized_rep_data, global_hash_ring_map[1], local_hash_ring_map[1], addr_request_map, mt, rid);
  }

  // send updates to storage nodes
  unordered_set<string> failed_keys;
  for (auto it = addr_request_map.begin(); it != addr_request_map.end(); it++) {
    bool succeed;
    auto res = send_request<communication::Request, communication::Response>(it->second, pushers[it->first], response_puller, succeed);

    if (!succeed) {
      logger->info("rep factor put timed out!");

      for (int i = 0; i < it->second.tuple_size(); i++) {
        vector<string> tokens;
        split(it->second.tuple(i).key(), '_', tokens);
        failed_keys.insert(tokens[1]);
      }
    } else {
      for (int i = 0; i < res.tuple_size(); i++) {
        if (res.tuple(i).err_number() == 2) {
          logger->info("Replication factor put for key {} rejected due to incorrect address.", res.tuple(i).key());

          vector<string> tokens;
          split(res.tuple(i).key(), '_', tokens);
          failed_keys.insert(tokens[1]);
        }
      }
    }
  }

  for (auto it = requests.begin(); it != requests.end(); it++) {
    string key = it->first;

    if (failed_keys.find(key) == failed_keys.end()) {
      for (unsigned tier = MIN_TIER; tier <= MAX_TIER; tier++) {
        unsigned rep = max(placement[key].global_replication_map_[tier], orig_placement_info[key].global_replication_map_[tier]);
        auto threads = responsible_global(key, rep, global_hash_ring_map[tier]);

        for (auto server_iter = threads.begin(); server_iter != threads.end(); server_iter++) {
          prepare_replication_factor_update(key, replication_factor_map, server_iter->get_replication_factor_change_connect_addr(), placement);
        }
      }

      // form placement requests for routing nodes
      for (auto routing_iter = routing_address.begin(); routing_iter != routing_address.end(); routing_iter++) {
        prepare_replication_factor_update(key, replication_factor_map, RoutingThread(*routing_iter, 0).get_replication_factor_change_connect_addr(), placement);
      }
    }
  }

  // send placement info update to all relevant nodes
  for (auto it = replication_factor_map.begin(); it != replication_factor_map.end(); it++) {
    string serialized_msg;
    it->second.SerializeToString(&serialized_msg);
    zmq_util::send_string(serialized_msg, &pushers[it->first]);
  }

  // restore rep factor for failed keys
  for (auto it = failed_keys.begin(); it != failed_keys.end(); it++) {
    placement[*it] = orig_placement_info[*it];
  }
}