#ifndef SRC_INCLUDE_HASH_RING_HPP_
#define SRC_INCLUDE_HASH_RING_HPP_

#include "hashers.hpp"
#include "utils/consistent_hash_map.hpp"

template <typename H>
class HashRing : public ConsistentHashMap<ServerThread, H> {
 public:
  HashRing() {}

  ~HashRing() {}

 public:
  std::pair<typename ConsistentHashMap<ServerThread, H>::iterator, bool> insert(
      ServerThread st) {
    auto result = ConsistentHashMap<ServerThread, H>::insert(st);

    if (result.second) {
      unique_servers.insert(st);
    }

    return result;
  }

  void erase(ServerThread st) {
    unique_servers.erase(st);
    ConsistentHashMap<ServerThread, H>::erase(st);
  }

  std::unordered_set<ServerThread, ThreadHash> get_unique_servers() {
    return unique_servers;
  }

 private:
  std::unordered_set<ServerThread, ThreadHash> unique_servers;
};

typedef HashRing<GlobalHasher> GlobalHashRing;
typedef HashRing<LocalHasher> LocalHashRing;

template <typename H>
bool insert_to_hash_ring(H& hash_ring, Address ip, unsigned tid) {
  bool succeed;

  for (unsigned virtual_num = 0; virtual_num < kVirtualThreadNum;
       virtual_num++) {
    succeed = hash_ring.insert(ServerThread(ip, tid, virtual_num)).second;
  }

  return succeed;
}

template <typename H>
void remove_from_hash_ring(H& hash_ring, Address ip, unsigned tid) {
  for (unsigned virtual_num = 0; virtual_num < kVirtualThreadNum;
       virtual_num++) {
    hash_ring.erase(ServerThread(ip, tid, virtual_num));
  }
}

ServerThreadSet responsible_global(const Key& key, unsigned global_rep,
                                   GlobalHashRing& global_hash_ring);

std::unordered_set<unsigned> responsible_local(const Key& key,
                                               unsigned local_rep,
                                               LocalHashRing& local_hash_ring);

ServerThreadSet get_responsible_threads_metadata(
    const Key& key, GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring);

ServerThreadSet get_responsible_threads(
    Address respond_address, const Key& key, bool metadata,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, KeyInfo>& placement, SocketCache& pushers,
    const std::vector<unsigned>& tier_ids, bool& succeed, unsigned& seed);

void issue_replication_factor_request(const Address& respond_address,
                                      const Key& key,
                                      GlobalHashRing& global_memory_hash_ring,
                                      LocalHashRing& local_memory_hash_ring,
                                      SocketCache& pushers, unsigned& seed);

std::vector<Address> get_address_from_routing(UserThread& ut, const Key& key,
                                              zmq::socket_t& sending_socket,
                                              zmq::socket_t& receiving_socket,
                                              bool& succeed, Address& ip,
                                              unsigned& thread_id,
                                              unsigned& rid);

RoutingThread get_random_routing_thread(std::vector<Address>& routing_address,
                                        unsigned& seed,
                                        unsigned& kRoutingThreadCount);

inline void warmup_placement_to_defaults(
    std::unordered_map<Key, KeyInfo>& placement,
    unsigned& kDefaultGlobalMemoryReplication,
    unsigned& kDefaultGlobalEbsReplication,
    unsigned& kDefaultLocalReplication) {
  for (unsigned i = 1; i <= 1000000; i++) {
    // key is 8 bytes
    Key key =
        std::string(8 - std::to_string(i).length(), '0') + std::to_string(i);
    placement[key].global_replication_map_[1] = kDefaultGlobalMemoryReplication;
    placement[key].global_replication_map_[2] = kDefaultGlobalEbsReplication;
    placement[key].local_replication_map_[1] = kDefaultLocalReplication;
    placement[key].local_replication_map_[2] = kDefaultLocalReplication;
  }
}

inline void init_replication(std::unordered_map<Key, KeyInfo>& placement,
                             const Key& key) {
  for (const unsigned& tier_id : kAllTierIds) {
    placement[key].global_replication_map_[tier_id] =
        kTierDataMap[tier_id].default_replication_;
    placement[key].local_replication_map_[tier_id] = kDefaultLocalReplication;
  }
}

#endif  // SRC_INCLUDE_HASH_RING_HPP_
