#include <chrono>
#include <fstream>

#include "common.hpp"
#include "hash_ring.hpp"
#include "kvs/kvs_handlers.hpp"
#include "kvs/rc_pair_lattice.hpp"
#include "zmq/socket_cache.hpp"

void user_request_handler(
    unsigned& total_access, unsigned& seed, zmq::socket_t* request_puller,
    std::chrono::system_clock::time_point& start_time,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<std::string, KeyStat>& key_stat_map,
    std::unordered_map<std::string,
                       std::pair<std::chrono::system_clock::time_point,
                                 std::vector<PendingRequest>>>&
        pending_request_map,
    std::unordered_map<
        std::string,
        std::multiset<std::chrono::time_point<std::chrono::system_clock>>>&
        key_access_timestamp,
    std::unordered_map<std::string, KeyInfo>& placement,
    std::unordered_set<std::string>& local_changeset, ServerThread& wt,
    Serializer* serializer, SocketCache& pushers) {
  std::string req_string = zmq_util::recv_string(request_puller);
  communication::Request req;
  req.ParseFromString(req_string);

  communication::Response response;
  std::string respond_id = "";

  if (req.has_request_id()) {
    respond_id = req.request_id();
    response.set_response_id(respond_id);
  }

  std::vector<unsigned> tier_ids = {kSelfTierId};
  bool succeed;

  if (req.type() == "GET") {
    for (int i = 0; i < req.tuple_size(); i++) {
      // first check if the thread is responsible for the key
      std::string key = req.tuple(i).key();
      auto threads = get_responsible_threads(
          wt.get_replication_factor_connect_addr(), key, is_metadata(key),
          global_hash_ring_map, local_hash_ring_map, placement, pushers,
          tier_ids, succeed, seed);

      if (succeed) {
        if (threads.find(wt) == threads.end()) {
          if (is_metadata(key)) {  // this means that this node is not
                                   // responsible for this metadata key
            communication::Response_Tuple* tp = response.add_tuple();

            tp->set_key(key);
            tp->set_err_number(2);
          } else {  // if we don't know what threads are responsible, we issue a
                    // rep factor request and make the request pending
            issue_replication_factor_request(
                wt.get_replication_factor_connect_addr(), key,
                global_hash_ring_map[1], local_hash_ring_map[1], pushers, seed);
            std::string val = "";

            if (pending_request_map.find(key) == pending_request_map.end()) {
              pending_request_map[key].first = std::chrono::system_clock::now();
            }

            pending_request_map[key].second.push_back(
                PendingRequest("G", "", req.respond_address(), respond_id));
          }
        } else {  // if we know what threads are responsible, we process the get
                  // and return
          communication::Response_Tuple* tp = response.add_tuple();
          tp->set_key(key);

          auto res = process_get(key, serializer);
          tp->set_value(res.first.reveal().value);
          tp->set_err_number(res.second);

          if (req.tuple(i).has_num_address() &&
              req.tuple(i).num_address() != threads.size()) {
            tp->set_invalidate(true);
          }

          key_access_timestamp[key].insert(std::chrono::system_clock::now());
          total_access += 1;
        }
      } else {
        if (pending_request_map.find(key) == pending_request_map.end()) {
          pending_request_map[key].first = std::chrono::system_clock::now();
        }

        pending_request_map[key].second.push_back(
            PendingRequest("G", "", req.respond_address(), respond_id));
      }
    }
  } else if (req.type() == "PUT") {
    for (int i = 0; i < req.tuple_size(); i++) {
      // first check if the thread is responsible for the key
      std::string key = req.tuple(i).key();
      auto threads = get_responsible_threads(
          wt.get_replication_factor_connect_addr(), key, is_metadata(key),
          global_hash_ring_map, local_hash_ring_map, placement, pushers,
          tier_ids, succeed, seed);

      if (succeed) {
        if (threads.find(wt) == threads.end()) {
          if (is_metadata(key)) {  // this means that this node is not
                                   // responsible this metadata
            communication::Response_Tuple* tp = response.add_tuple();

            tp->set_key(key);
            tp->set_err_number(2);
          } else {  // if it's regular data, we don't know the replication
                    // factor, so ask
            issue_replication_factor_request(
                wt.get_replication_factor_connect_addr(), key,
                global_hash_ring_map[1], local_hash_ring_map[1], pushers, seed);

            if (pending_request_map.find(key) == pending_request_map.end()) {
              pending_request_map[key].first = std::chrono::system_clock::now();
            }

            if (req.has_respond_address()) {
              pending_request_map[key].second.push_back(
                  PendingRequest("P", req.tuple(i).value(),
                                 req.respond_address(), respond_id));
            } else {
              pending_request_map[key].second.push_back(
                  PendingRequest("P", req.tuple(i).value(), "", respond_id));
            }
          }
        } else {  // if we are the responsible party, insert this key
          communication::Response_Tuple* tp = response.add_tuple();
          tp->set_key(key);

          auto time_diff =
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now() - start_time)
                  .count();
          auto ts = generate_timestamp(time_diff, wt.get_tid());

          process_put(key, ts, req.tuple(i).value(), serializer, key_stat_map);
          tp->set_err_number(0);

          if (req.tuple(i).has_num_address() &&
              req.tuple(i).num_address() != threads.size()) {
            tp->set_invalidate(true);
          }

          key_access_timestamp[key].insert(std::chrono::system_clock::now());
          total_access += 1;
          local_changeset.insert(key);
        }
      } else {
        if (pending_request_map.find(key) == pending_request_map.end()) {
          pending_request_map[key].first = std::chrono::system_clock::now();
        }

        if (req.has_respond_address()) {
          pending_request_map[key].second.push_back(PendingRequest(
              "P", req.tuple(i).value(), req.respond_address(), respond_id));
        } else {
          pending_request_map[key].second.push_back(
              PendingRequest("P", req.tuple(i).value(), "", respond_id));
        }
      }
    }
  }

  if (response.tuple_size() > 0 && req.has_respond_address()) {
    std::string serialized_response;
    response.SerializeToString(&serialized_response);
    zmq_util::send_string(serialized_response, &pushers[req.respond_address()]);
  }
}
