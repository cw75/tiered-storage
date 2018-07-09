#include "hash_ring.hpp"
#include "spdlog/spdlog.h"

using namespace std;

void address_handler(
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* key_address_puller,
    SocketCache& pushers, RoutingThread& rt,
    unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    unordered_map<string, KeyInfo>& placement,
    unordered_map<string, pair<chrono::system_clock::time_point,
                               vector<pair<string, string>>>>&
        pending_key_request_map,
    unsigned& seed) {
  logger->info("Received key address request.");
  string serialized_key_req = zmq_util::recv_string(key_address_puller);
  communication::Key_Request key_req;
  key_req.ParseFromString(serialized_key_req);

  communication::Key_Response key_res;
  key_res.set_response_id(key_req.request_id());
  bool succeed;

  int num_servers = 0;
  for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end();
       ++it) {
    num_servers += it->second.size();
  }

  if (num_servers == 0) {
    key_res.set_err_number(1);

    string serialized_key_res;
    key_res.SerializeToString(&serialized_key_res);

    zmq_util::send_string(serialized_key_res,
                          &pushers[key_req.respond_address()]);
  } else {  // if there are servers, attempt to return the correct threads
    for (int i = 0; i < key_req.keys_size(); i++) {
      vector<unsigned> tier_ids;
      tier_ids.push_back(1);

      string key = key_req.keys(i);
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

        communication::Key_Response_Tuple* tp = key_res.add_tuple();
        tp->set_key(key);

        for (auto it = threads.begin(); it != threads.end(); it++) {
          tp->add_addresses(it->get_request_pulling_connect_addr());
        }
      } else {
        if (pending_key_request_map.find(key) ==
            pending_key_request_map.end()) {
          pending_key_request_map[key].first = chrono::system_clock::now();
        }

        pending_key_request_map[key].second.push_back(pair<string, string>(
            key_req.respond_address(), key_req.request_id()));
      }
    }

    if (key_res.tuple_size() > 0) {
      key_res.set_err_number(0);

      string serialized_key_res;
      key_res.SerializeToString(&serialized_key_res);

      zmq_util::send_string(serialized_key_res,
                            &pushers[key_req.respond_address()]);
    }
  }
}