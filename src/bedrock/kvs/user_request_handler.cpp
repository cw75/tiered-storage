#include <chrono>

#include "kvs/kvs_handlers.hpp"

void user_request_handler(
    unsigned& total_access, unsigned& seed, zmq::socket_t* request_puller,
    std::chrono::system_clock::time_point& start_time,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, unsigned>& key_size_map,
    PendingMap<PendingRequest>& pending_request_map,
    std::unordered_map<
        Key,
        std::multiset<std::chrono::time_point<std::chrono::system_clock>>>&
        key_access_timestamp,
    std::unordered_map<Key, KeyInfo>& placement,
    std::unordered_set<Key>& local_changeset, ServerThread& wt,
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

  bool succeed;

  // TODO(vikram): this can be refactored / cleaned up
  if (req.type() == "GET") {
    for (const auto& tuple : req.tuple()) {
      // first check if the thread is responsible for the key
      Key key = tuple.key();
      ServerThreadSet threads = get_responsible_threads(
          wt.get_replication_factor_connect_addr(), key, is_metadata(key),
          global_hash_ring_map, local_hash_ring_map, placement, pushers,
          kSelfTierIdVector, succeed, seed);

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

            pending_request_map[key].push_back(
                PendingRequest("G", "", req.respond_address(), respond_id));
          }
        } else {  // if we know the responsible threads, we process the request
          communication::Response_Tuple* tp = response.add_tuple();
          tp->set_key(key);

          auto res = process_get(key, serializer);
          tp->set_value(res.first.reveal().value);
          tp->set_err_number(res.second);

          if (tuple.has_num_address() &&
              tuple.num_address() != threads.size()) {
            tp->set_invalidate(true);
          }

          key_access_timestamp[key].insert(std::chrono::system_clock::now());
          total_access += 1;
        }
      } else {
        pending_request_map[key].push_back(
            PendingRequest("G", "", req.respond_address(), respond_id));
      }
    }
  } else if (req.type() == "PUT") {
    for (const auto& tuple : req.tuple()) {
      // first check if the thread is responsible for the key
      Key key = tuple.key();
      ServerThreadSet threads = get_responsible_threads(
          wt.get_replication_factor_connect_addr(), key, is_metadata(key),
          global_hash_ring_map, local_hash_ring_map, placement, pushers,
          kSelfTierIdVector, succeed, seed);

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

            if (req.has_respond_address()) {
              pending_request_map[key].push_back(
                  PendingRequest("P", tuple.value(),
                                 req.respond_address(), respond_id));
            } else {
              pending_request_map[key].push_back(
                  PendingRequest("P", tuple.value(), "", respond_id));
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

          process_put(key, ts, tuple.value(), serializer, key_size_map);
          tp->set_err_number(0);

          if (tuple.has_num_address() &&
              tuple.num_address() != threads.size()) {
            tp->set_invalidate(true);
          }

          key_access_timestamp[key].insert(std::chrono::system_clock::now());
          total_access += 1;
          local_changeset.insert(key);
        }
      } else {
        if (req.has_respond_address()) {
          pending_request_map[key].push_back(PendingRequest(
              "P", tuple.value(), req.respond_address(), respond_id));
        } else {
          pending_request_map[key].push_back(
              PendingRequest("P", tuple.value(), "", respond_id));
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
