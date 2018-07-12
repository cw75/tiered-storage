#include "kvs/kvs_handlers.hpp"

void send_gossip(AddressKeysetMap& addr_keyset_map, SocketCache& pushers,
                 Serializer* serializer) {
  std::unordered_map<Address, communication::Request> gossip_map;

  for (const auto& key_pair : addr_keyset_map) {
    std::string key = key_pair.first;
    gossip_map[key].set_type("PUT");

    for (const auto& address : key_pair.second) {
      auto res = process_get(address, serializer);

      if (res.second == 0) {
        prepare_put_tuple(gossip_map[key], address, res.first.reveal().value,
                          res.first.reveal().timestamp);
      }
    }
  }

  // send gossip
  for (const auto& gossip_pair : gossip_map) {
    push_request(gossip_pair.second, pushers[gossip_pair.first]);
  }
}

std::pair<ReadCommittedPairLattice<std::string>, unsigned> process_get(
    const Key& key, Serializer* serializer) {
  unsigned err_number = 0;
  auto res = serializer->get(key, err_number);

  // check if the value is an empty string
  if (res.reveal().value == "") {
    err_number = 1;
  }

  return std::pair<ReadCommittedPairLattice<std::string>, unsigned>(res,
                                                                    err_number);
}

void process_put(const Key& key, const unsigned long long& timestamp,
                 const std::string& value, Serializer* serializer,
                 std::unordered_map<Key, unsigned>& key_size_map) {
  if (serializer->put(key, value, timestamp)) {
    // update value size if the value is replaced
    key_size_map[key] = value.size();
  }
}

bool is_primary_replica(const Key& key, std::unordered_map<Key, KeyInfo>& placement,
                        std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
                        std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
                        ServerThread& st) {
  if (placement[key].global_replication_map_[kSelfTierId] == 0) {
    return false;
  } else if (kSelfTierId == 2 && placement[key].global_replication_map_[1] > 0) {
    return false;
  } else {
    auto global_pos = global_hash_ring_map[kSelfTierId].find(key);
    if (global_pos != global_hash_ring_map[kSelfTierId].end() &&
        st.get_ip().compare(global_pos->second.get_ip()) == 0) {
      auto local_pos = local_hash_ring_map[kSelfTierId].find(key);
      if (local_pos != local_hash_ring_map[kSelfTierId].end() &&
         st.get_tid() == local_pos->second.get_tid()) {
        return true;
      }
    }
  }
  return false;
}
