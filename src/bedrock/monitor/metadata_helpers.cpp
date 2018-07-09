#include "hash_ring.hpp"
#include "spdlog/spdlog.h"

using namespace std;

string prepare_metadata_request(
    const string& key, GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring,
    unordered_map<string, communication::Request>& addr_request_map,
    MonitoringThread& mt, unsigned& rid, string type) {
  auto threads = get_responsible_threads_metadata(key, global_memory_hash_ring,
                                                  local_memory_hash_ring);
  if (threads.size() != 0) {
    string target_address = next(begin(threads), rand() % threads.size())
                                ->get_request_pulling_connect_addr();
    if (addr_request_map.find(target_address) == addr_request_map.end()) {
      addr_request_map[target_address].set_type(type);
      addr_request_map[target_address].set_respond_address(
          mt.get_request_pulling_connect_addr());
      string req_id = mt.get_ip() + ":" + to_string(rid);
      addr_request_map[target_address].set_request_id(req_id);
      rid += 1;
    }

    return target_address;
  }

  return string();
}

void prepare_metadata_get_request(
    const string& key, GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring,
    unordered_map<string, communication::Request>& addr_request_map,
    MonitoringThread& mt, unsigned& rid) {
  string target_address = prepare_metadata_request(
      key, global_memory_hash_ring, local_memory_hash_ring, addr_request_map,
      mt, rid, "GET");

  if (!target_address.empty()) {
    prepare_get_tuple(addr_request_map[target_address], key);
  }
}

void prepare_metadata_put_request(
    const string& key, const string& value,
    GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring,
    unordered_map<string, communication::Request>& addr_request_map,
    MonitoringThread& mt, unsigned& rid) {
  string target_address = prepare_metadata_request(
      key, global_memory_hash_ring, local_memory_hash_ring, addr_request_map,
      mt, rid, "PUT");

  if (!target_address.empty()) {
    prepare_put_tuple(addr_request_map[target_address], key, value, 0);
  }
}