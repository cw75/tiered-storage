#include <chrono>
#include <fstream>

#include "common.hpp"
#include "hash_ring.hpp"
#include "kvs/kvs_handlers.hpp"
#include "kvs/rc_pair_lattice.hpp"
#include "zmq/socket_cache.hpp"

void rep_factor_response_handler(
    unsigned& seed, unsigned& total_access,
    std::shared_ptr<spdlog::logger> logger,
    zmq::socket_t* rep_factor_response_puller,
    std::chrono::system_clock::time_point& start_time,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<std::string,
                       std::pair<std::chrono::system_clock::time_point,
                                 std::vector<PendingRequest>>>&
        pending_request_map,
    std::unordered_map<std::string,
                       std::pair<std::chrono::system_clock::time_point,
                                 std::vector<PendingGossip>>>
        pending_gossip_map,
    std::unordered_map<
        std::string,
        std::multiset<std::chrono::time_point<std::chrono::system_clock>>>&
        key_access_timestamp,
    std::unordered_map<std::string, KeyInfo> placement,
    std::unordered_map<std::string, unsigned>& key_size_map,
    std::unordered_set<std::string>& local_changeset, ServerThread& wt,
    Serializer* serializer, SocketCache& pushers) {
  std::string response_string =
      zmq_util::recv_string(rep_factor_response_puller);
  communication::Response response;
  response.ParseFromString(response_string);

  std::vector<std::string> tokens;
  split(response.tuple(0).key(), '_', tokens);
  std::string key = tokens[1];

  if (response.tuple(0).err_number() == 2) {
    auto respond_address = wt.get_replication_factor_connect_addr();
    issue_replication_factor_request(respond_address, key,
                                     global_hash_ring_map[1],
                                     local_hash_ring_map[1], pushers, seed);
    return;
  } else if (response.tuple(0).err_number() == 0) {
    communication::Replication_Factor rep_data;
    rep_data.ParseFromString(response.tuple(0).value());

    for (const auto& global : rep_data.global()) {
      placement[key].global_replication_map_[global.tier_id()] =
          global.global_replication();
    }

    for (const auto& local : rep_data.local()) {
      placement[key].local_replication_map_[local.tier_id()] =
          local.local_replication();
    }
  } else {
    for (const unsigned& tier_id : kAllTierIds) {
      placement[key].global_replication_map_[tier_id] =
          kTierDataMap[tier_id].default_replication_;
      placement[key].local_replication_map_[tier_id] = kDefaultLocalReplication;
    }
  }

  bool succeed;

  if (pending_request_map.find(key) != pending_request_map.end()) {
    ServerThreadSet threads = get_responsible_threads(
        wt.get_replication_factor_connect_addr(), key, is_metadata(key),
        global_hash_ring_map, local_hash_ring_map, placement, pushers, kSelfTierIdVector,
        succeed, seed);

    if (succeed) {
      bool responsible = threads.find(wt) != threads.end();
      std::vector<PendingRequest> request_vec = pending_request_map[key].second;

      for (const PendingRequest& request : request_vec) {
        auto now = std::chrono::system_clock::now();

        if (!responsible && request.addr_ != "") {
          communication::Response response;

          if (request.respond_id_ != "") {
            response.set_response_id(request.respond_id_);
          }

          communication::Response_Tuple* tp = response.add_tuple();
          tp->set_key(key);
          tp->set_err_number(2);

          for (const ServerThread& thread : threads) {
            tp->add_addresses(thread.get_request_pulling_connect_addr());
          }

          std::string serialized_response;
          response.SerializeToString(&serialized_response);
          zmq_util::send_string(serialized_response, &pushers[request.addr_]);
        } else if (responsible && request.addr_ == "") {
          // only put requests should fall into this category
          if (request.type_ == "P") {
            auto time_diff =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - start_time)
                    .count();
            auto ts = generate_timestamp(time_diff, wt.get_tid());

            process_put(key, ts, request.value_, serializer, key_size_map);
            key_access_timestamp[key].insert(now);

            total_access += 1;
            local_changeset.insert(key);
          } else {
            logger->error("Received a GET request with no response address.");
          }
        } else if (responsible && request.addr_ != "") {
          communication::Response response;

          if (request.respond_id_ != "") {
            response.set_response_id(request.respond_id_);
          }

          communication::Response_Tuple* tp = response.add_tuple();
          tp->set_key(key);

          if (request.type_ == "G") {
            auto res = process_get(key, serializer);
            tp->set_value(res.first.reveal().value);
            tp->set_err_number(res.second);

            key_access_timestamp[key].insert(std::chrono::system_clock::now());
            total_access += 1;
          } else {
            auto time_diff =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - start_time)
                    .count();
            auto ts = generate_timestamp(time_diff, wt.get_tid());

            process_put(key, ts, request.value_, serializer, key_size_map);
            tp->set_err_number(0);

            key_access_timestamp[key].insert(now);
            total_access += 1;
            local_changeset.insert(key);
          }

          std::string serialized_response;
          response.SerializeToString(&serialized_response);
          zmq_util::send_string(serialized_response, &pushers[request.addr_]);
        }
      }
    } else {
      logger->error(
          "Missing key replication factor in process pending request routine.");
    }

    pending_request_map.erase(key);
  }

  if (pending_gossip_map.find(key) != pending_gossip_map.end()) {
    auto threads = get_responsible_threads(
        wt.get_replication_factor_connect_addr(), key, is_metadata(key),
        global_hash_ring_map, local_hash_ring_map, placement, pushers, kSelfTierIdVector,
        succeed, seed);

    if (succeed) {
      if (threads.find(wt) != threads.end()) {
        // TODO(vikram): change once Chenggang finishes type refactor
        for (auto it = pending_gossip_map[key].second.begin();
             it != pending_gossip_map[key].second.end(); ++it) {
          process_put(key, it->ts_, it->value_, serializer, key_size_map);
        }
      } else {
        std::unordered_map<std::string, communication::Request> gossip_map;

        // forward the gossip
        for (auto it = threads.begin(); it != threads.end(); it++) {
          gossip_map[it->get_gossip_connect_addr()].set_type("PUT");

          for (auto iter = pending_gossip_map[key].second.begin();
               iter != pending_gossip_map[key].second.end(); iter++) {
            prepare_put_tuple(gossip_map[it->get_gossip_connect_addr()], key,
                              iter->value_, iter->ts_);
          }
        }

        // redirect gossip
        for (auto it = gossip_map.begin(); it != gossip_map.end(); it++) {
          push_request(it->second, pushers[it->first]);
        }
      }
    } else {
      logger->error(
          "Missing key replication factor in process pending gossip routine.");
    }

    pending_gossip_map.erase(key);
  }
}
