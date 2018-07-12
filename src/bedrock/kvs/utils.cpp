#include "kvs/kvs_handlers.hpp"

void send_gossip(AddressKeysetMap& addr_keyset_map, SocketCache& pushers,
                 Serializer* serializer) {
  std::unordered_map<Address, KeyRequest> gossip_map;

  for (const auto& key_pair : addr_keyset_map) {
    std::string key = key_pair.first;
    RequestType type;
    RequestType_Parse("GET", &type);
    gossip_map[key].set_type(type);

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
