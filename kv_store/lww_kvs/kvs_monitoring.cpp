#include <zmq.hpp>
#include <string>
#include <stdlib.h>
#include <sstream>
#include <fstream>
#include <vector>
#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include <memory>
#include "rc_kv_store.h"
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"
#include "consistent_hash_map.hpp"
#include "common.h"

// number of nodes to add concurrently
#define NODE_ADD 3

using namespace std;
using address_t = string;

// read-only per-tier metadata
unordered_map<unsigned, tier_data> tier_data_map;

bool ping(
    string key,
    string value,
    SocketCache& pushers,
    shared_ptr<spdlog::logger> logger,
    monitoring_thread_t& mt,
    zmq::socket_t& response_puller,
    unordered_map<unsigned, global_hash_t>& global_hash_ring_map,
    unordered_map<unsigned, local_hash_t>& local_hash_ring_map,
    unordered_map<string, key_info>& placement) {
  communication::Request req;
  req.set_respond_address(mt.get_request_pulling_connect_addr());
  if (value == "") {
    // get request
    req.set_type("GET");
    prepare_get_tuple(req, key);
  } else {
    // put request
    req.set_type("PUT");
    prepare_put_tuple(req, key, value, 0);
  }
  if (placement.find(key) != placement.end()) {
    unordered_set<server_thread_t, thread_hash> threads;
    // hard code memory tier for now
    unsigned tier_id = 1;
    auto mts = responsible_global(key, placement[key].global_replication_map_[tier_id], global_hash_ring_map[tier_id]);
    for (auto it = mts.begin(); it != mts.end(); it++) {
      string ip = it->get_ip();
      if (placement[key].local_replication_map_.find(ip) == placement[key].local_replication_map_.end()) {
        placement[key].local_replication_map_[ip] = DEFAULT_LOCAL_REPLICATION;
      }
      auto tids = responsible_local(key, placement[key].local_replication_map_[ip], local_hash_ring_map[tier_id]);
      for (auto iter = tids.begin(); iter != tids.end(); iter++) {
        threads.insert(server_thread_t(ip, *iter));
      }
    }
    auto rand_thread = *(next(begin(threads), rand() % threads.size()));
    string worker_address = rand_thread.get_request_pulling_connect_addr();
    bool succeed;
    auto res = send_request<communication::Request, communication::Response>(req, pushers[worker_address], response_puller, succeed);
    if (succeed) {
      if (res.tuple(0).err_number() == 2) {
        logger->info("hash ring inconsistency");
        return false;
      } else {
        return true;
      }
    } else {
      logger->info("request timed out when querying worker");
      return false;
    }
  } else {
    logger->info("Error: missing replication factor for monitoring node");
    return false;
  }
}

void prepare_metadata_get_request(
    string& key,
    global_hash_t& global_memory_hash_ring,
    local_hash_t& local_memory_hash_ring,
    unordered_map<address_t, communication::Request>& addr_request_map,
    monitoring_thread_t& mt) {
  auto threads = get_responsible_threads_metadata(key, global_memory_hash_ring, local_memory_hash_ring);
  if (threads.size() != 0) {
    string target_address = next(begin(threads), rand() % threads.size())->get_request_pulling_connect_addr();
    if (addr_request_map.find(target_address) == addr_request_map.end()) {
      addr_request_map[target_address].set_type("GET");
      addr_request_map[target_address].set_respond_address(mt.get_request_pulling_connect_addr());
    }
    prepare_get_tuple(addr_request_map[target_address], key);
  }
}

void prepare_metadata_put_request(
    string& key,
    string& value,
    global_hash_t& global_memory_hash_ring,
    local_hash_t& local_memory_hash_ring,
    unordered_map<address_t, communication::Request>& addr_request_map) {
  auto threads = get_responsible_threads_metadata(key, global_memory_hash_ring, local_memory_hash_ring);
  if (threads.size() != 0) {
    string target_address = next(begin(threads), rand() % threads.size())->get_request_pulling_connect_addr();
    if (addr_request_map.find(target_address) == addr_request_map.end()) {
      addr_request_map[target_address].set_type("PUT");
    }
    prepare_put_tuple(addr_request_map[target_address], key, value, 0);
  }
}

void prepare_replication_factor_update(
    string& key,
    unordered_map<address_t, communication::Replication_Factor_Request>& replication_factor_map,
    string server_address,
    unordered_map<string, key_info>& placement,
    unordered_map<address_t, unsigned>& local_replication_map) {
  communication::Replication_Factor_Request_Tuple* tp = replication_factor_map[server_address].add_tuple();
  tp->set_key(key);
  for (auto iter = placement[key].global_replication_map_.begin(); iter != placement[key].global_replication_map_.end(); iter++) {
    communication::Replication_Factor_Request_Global* g = tp->add_global();
    g->set_tier_id(iter->first);
    g->set_global_replication(iter->second);
  }
  for (auto iter = local_replication_map.begin(); iter != local_replication_map.end(); iter++) {
    communication::Replication_Factor_Request_Local* l = tp->add_local();
    l->set_ip(iter->first);
    l->set_local_replication(iter->second);
  }
}

// assume the caller has the replication factor for the keys and the requests are valid
// (rep factor <= total number of nodes in a tier)
void change_replication_factor(
    unordered_map<string, key_info>& requests,
    unordered_map<unsigned, global_hash_t>& global_hash_ring_map,
    unordered_map<unsigned, local_hash_t>& local_hash_ring_map,
    vector<address_t>& proxy_address,
    unordered_map<string, key_info>& placement,
    SocketCache& pushers) {
  // used to keep track of the original replication factors for the requested keys
  unordered_map<string, key_info> orig_placement_info;

  // form the placement request map
  unordered_map<address_t, communication::Replication_Factor_Request> replication_factor_map;

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
    // form rep factor change requests for all tiers
    for (unsigned tier = MIN_TIER; tier <= MAX_TIER; tier++) {
      unsigned rep = max(placement[key].global_replication_map_[tier], orig_placement_info[key].global_replication_map_[tier]);
      auto threads = responsible_global(key, rep, global_hash_ring_map[tier]);
      for (auto server_iter = threads.begin(); server_iter != threads.end(); server_iter++) {
        prepare_replication_factor_update(key, replication_factor_map, server_iter->get_replication_factor_change_connect_addr(), placement, it->second.local_replication_map_);
      }
    }

    //TODO: check the local_replication map again in case we have missing entry for global rep factors?

    // form placement requests for proxy nodes
    for (auto proxy_iter = proxy_address.begin(); proxy_iter != proxy_address.end(); proxy_iter++) {
      prepare_replication_factor_update(key, replication_factor_map, proxy_thread_t(*proxy_iter, 0).get_replication_factor_change_connect_addr(), placement, it->second.local_replication_map_);
    }
  }

  // send placement info update to all relevant nodes
  for (auto it = replication_factor_map.begin(); it != replication_factor_map.end(); it++) {
    string serialized_msg;
    it->second.SerializeToString(&serialized_msg);
    zmq_util::send_string(serialized_msg, &pushers[it->first]);
  }

  // store the new replication factor in storage servers
  // TODO: make this synchronous?
  unordered_map<address_t, communication::Request> addr_request_map;
  for (auto it = requests.begin(); it != requests.end(); it++) {
    string key = it->first;
    communication::Replication_Factor rep_data;
    for (auto iter = placement[key].global_replication_map_.begin(); iter != placement[key].global_replication_map_.end(); iter++) {
      communication::Replication_Factor_Global* g = rep_data.add_global();
      g->set_tier_id(iter->first);
      g->set_global_replication(iter->second);
    }
    for (auto iter = placement[key].local_replication_map_.begin(); iter != placement[key].local_replication_map_.end(); iter++) {
      communication::Replication_Factor_Local* l = rep_data.add_local();
      l->set_ip(iter->first);
      l->set_local_replication(iter->second);
    }
    string rep_key = key + "_replication";
    string serialized_rep_data;
    rep_data.SerializeToString(&serialized_rep_data);
    prepare_metadata_put_request(rep_key, serialized_rep_data, global_hash_ring_map[1], local_hash_ring_map[1], addr_request_map);
  }

  for (auto it = addr_request_map.begin(); it != addr_request_map.end(); it++) {
    push_request(it->second, pushers[it->first]);
  }
}

int main(int argc, char* argv[]) {

  auto logger = spdlog::basic_logger_mt("basic_logger", "log.txt", true);
  logger->flush_on(spdlog::level::info); 

  if (argc != 1) {
    cerr << "usage:" << argv[0] << endl;
    return 1;
  }

  string ip = get_ip("monitoring");

  tier_data_map[1] = tier_data(MEMORY_THREAD_NUM, DEFAULT_GLOBAL_MEMORY_REPLICATION);
  tier_data_map[2] = tier_data(EBS_THREAD_NUM, DEFAULT_GLOBAL_EBS_REPLICATION);

  // initialize hash ring maps
  unordered_map<unsigned, global_hash_t> global_hash_ring_map;
  unordered_map<unsigned, local_hash_t> local_hash_ring_map;

  // form local hash rings
  for (auto it = tier_data_map.begin(); it != tier_data_map.end(); it++) {
    for (unsigned tid = 0; tid < it->second.thread_number_; tid++) {
      insert_to_hash_ring<local_hash_t>(local_hash_ring_map[it->first], ip, tid);
    }
  }

  // keep track of the keys' replication info
  unordered_map<string, key_info> placement;
  // warm up for benchmark
  warmup(placement);

  // keep track of the keys' access by worker address
  unordered_map<string, unordered_map<address_t, unsigned>> key_access_frequency;
  // keep track of the keys' access summary
  unordered_map<string, unsigned> key_access_summary;
  // keep track of memory tier storage consumption
  unordered_map<address_t, unordered_map<unsigned, unsigned long long>> memory_tier_storage;
  // keep track of ebs tier storage consumption
  unordered_map<address_t, unordered_map<unsigned, unsigned long long>> ebs_tier_storage;
  // keep track of memory tier thread occupancy
  unordered_map<address_t, unordered_map<unsigned, pair<double, unsigned>>> memory_tier_occupancy;
  // keep track of ebs tier thread occupancy
  unordered_map<address_t, unordered_map<unsigned, pair<double, unsigned>>> ebs_tier_occupancy;
  // keep track of memory tier key access frequency
  unordered_map<address_t, unordered_map<unsigned, unordered_map<string, unsigned>>> memory_tier_key_access;
  // keep track of ebs tier key access frequency
  unordered_map<address_t, unordered_map<unsigned, unordered_map<string, unsigned>>> ebs_tier_key_access;
  // keep track of user latency info
  unordered_map<address_t, double> user_latency;
  // read in the initial server addresses and build the hash ring
  string ip_line;
  ifstream address;

  vector<address_t> proxy_address;

  // read address of management node from conf file
  address_t management_address;

  address.open("conf/monitoring/management_ip.txt");
  getline(address, ip_line);
  management_address = ip_line;
  address.close();

  monitoring_thread_t mt = monitoring_thread_t(ip);

  zmq::context_t context(1);

  SocketCache pushers(&context, ZMQ_PUSH);

  // responsible for both node join and departure
  zmq::socket_t response_puller(context, ZMQ_PULL);
  int timeout = 5000;
  response_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  response_puller.bind(mt.get_request_pulling_bind_addr());

  unordered_map<address_t, unsigned> departing_node_map;

  // responsible for both node join and departure
  zmq::socket_t notify_puller(context, ZMQ_PULL);
  notify_puller.bind(mt.get_notify_bind_addr());
  // responsible for receiving depart done notice
  zmq::socket_t depart_done_puller(context, ZMQ_PULL);
  depart_done_puller.bind(mt.get_depart_done_bind_addr());
  // responsible for receiving latency update from users
  zmq::socket_t latency_puller(context, ZMQ_PULL);
  latency_puller.bind(mt.get_latency_report_bind_addr());

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(notify_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(depart_done_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(latency_puller), 0, ZMQ_POLLIN, 0 }
  };

  auto report_start = chrono::system_clock::now();
  auto report_end = chrono::system_clock::now();

  auto freeze_start = chrono::system_clock::now();

  unsigned adding_memory_node = 0;
  bool adding_ebs_node = false;
  bool removing_memory_node = false;
  bool removing_ebs_node = false;

  unsigned server_monitoring_epoch = 0;

  double average_memory_consumption = 0;
  double average_ebs_consumption = 0;
  double average_memory_consumption_new = 0;
  double average_ebs_consumption_new = 0;

  while (true) {
    // listen for ZMQ events
    zmq_util::poll(0, &pollitems);

    // handle a join or depart event
    if (pollitems[0].revents & ZMQ_POLLIN) {
      string message = zmq_util::recv_string(&notify_puller);

      vector<string> v;
      split(message, ':', v);
      string type = v[0];
      unsigned tier = stoi(v[1]);
      string new_server_ip = v[2];
      if (type == "join") {
        logger->info("received join");
        logger->info("new server ip is {}", new_server_ip);
        logger->info("tier id is {}", to_string(tier));
        if (tier == 1) {
          insert_to_hash_ring<global_hash_t>(global_hash_ring_map[tier], new_server_ip, 0);
          if (adding_memory_node > 0) {
            adding_memory_node -= 1;
          }
          // reset timer
          freeze_start = chrono::system_clock::now();
        } else if (tier == 2) {
          insert_to_hash_ring<global_hash_t>(global_hash_ring_map[tier], new_server_ip, 0);
          adding_ebs_node = false;
          // reset timer
          freeze_start = chrono::system_clock::now();
        } else if (tier == 0) {
          proxy_address.push_back(new_server_ip);
        } else {
          cerr << "Invalid Tier info\n";
        }
        for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
          logger->info("hash ring for tier {} size is {}", to_string(it->first), to_string(it->second.size()));
        }
      } else if (type == "depart") {
        logger->info("received depart");
        logger->info("departing server ip is {}", new_server_ip);
        //cerr << "received depart\n";
        // update hash ring
        if (tier == 1) {
          remove_from_hash_ring<global_hash_t>(global_hash_ring_map[tier], new_server_ip, 0);
          memory_tier_storage.erase(new_server_ip);
          memory_tier_occupancy.erase(new_server_ip);
          for (auto it = key_access_frequency.begin(); it != key_access_frequency.end(); it++) {
            for (unsigned i = 0; i < MEMORY_THREAD_NUM; i++) {
              it->second.erase(new_server_ip + ":" + to_string(i));
            }
          }
        } else if (tier == 2) {
          remove_from_hash_ring<global_hash_t>(global_hash_ring_map[tier], new_server_ip, 0);
          ebs_tier_storage.erase(new_server_ip);
          ebs_tier_occupancy.erase(new_server_ip);
          for (auto it = key_access_frequency.begin(); it != key_access_frequency.end(); it++) {
            for (unsigned i = 0; i < EBS_THREAD_NUM; i++) {
              it->second.erase(new_server_ip + ":" + to_string(i));
            }
          }
        } else {
          cerr << "Invalid Tier info\n";
        }
        for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
          logger->info("hash ring for tier {} size is {}", to_string(it->first), to_string(it->second.size()));
        }
      }
    }

    if (pollitems[1].revents & ZMQ_POLLIN) {
      string msg = zmq_util::recv_string(&depart_done_puller);
      vector<string> tokens;
      split(msg, '_', tokens);
      address_t departed_ip = tokens[0];
      unsigned tier_id = stoi(tokens[1]);
      if (departing_node_map.find(departed_ip) != departing_node_map.end()) {
        departing_node_map[departed_ip] -= 1;
        if (departing_node_map[departed_ip] == 0) {
          if (tier_id == 1) {
            logger->info("removing memory node {}", departed_ip);
            string shell_command = "curl -X POST http://" + management_address + "/remove/memory/" + departed_ip;
            system(shell_command.c_str());
            removing_memory_node = false;
          } else {
            logger->info("removing ebs node {}", departed_ip);
            string shell_command = "curl -X POST http://" + management_address + "/remove/ebs/" + departed_ip;
            system(shell_command.c_str());
            removing_ebs_node = false;
          }
          // reset timer
          freeze_start = chrono::system_clock::now();
          departing_node_map.erase(departed_ip);
        }
      } else {
        cerr << "missing entry in the depart done map\n";
      }
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      string serialized_latency = zmq_util::recv_string(&latency_puller);
      communication::Latency l;
      l.ParseFromString(serialized_latency);
      if (l.has_finish() && l.finish()) {
        user_latency.erase(l.uid());
      } else {
        user_latency[l.uid()] = l.latency();
      }
    }

    report_end = std::chrono::system_clock::now();

    if (chrono::duration_cast<std::chrono::microseconds>(report_end-report_start).count() >= MONITORING_THRESHOLD) {
      server_monitoring_epoch += 1;

      unordered_map<address_t, communication::Request> addr_request_map;

      for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
        unsigned tier_id = it->first;
        auto hash_ring = &(it->second);
        unordered_set<string> observed_ip;
        for (auto iter = hash_ring->begin(); iter != hash_ring->end(); iter++) {
          if (observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
            for (unsigned i = 0; i < tier_data_map[tier_id].thread_number_; i++) {
              string key = iter->second.get_ip() + "_" + to_string(i) + "_" + to_string(tier_id) + "_stat";
              prepare_metadata_get_request(key, global_hash_ring_map[1], local_hash_ring_map[1], addr_request_map, mt);
              key = iter->second.get_ip() + "_" + to_string(i) + "_" + to_string(tier_id) + "_access";
              prepare_metadata_get_request(key, global_hash_ring_map[1], local_hash_ring_map[1], addr_request_map, mt);
            }
            observed_ip.insert(iter->second.get_ip());
          }
        }
      }

      for (auto it = addr_request_map.begin(); it != addr_request_map.end(); it++) {
        bool succeed;
        auto res = send_request<communication::Request, communication::Response>(it->second, pushers[it->first], response_puller, succeed);
        if (succeed) {
          for (int i = 0; i < res.tuple_size(); i++) {
            if (res.tuple(i).err_number() == 0) {
              vector<string> tokens;
              split(res.tuple(i).key(), '_', tokens);
              string ip = tokens[0];
              unsigned tid = stoi(tokens[1]);
              unsigned tier_id = stoi(tokens[2]);
              string metadata_type = tokens[3];

              if (metadata_type == "stat") {
                // deserialized the value
                communication::Server_Stat stat;
                stat.ParseFromString(res.tuple(i).value());
                if (tier_id == 1) {
                  memory_tier_storage[ip][tid] = stat.storage_consumption();
                  memory_tier_occupancy[ip][tid] = pair<double, unsigned>(stat.occupancy(), stat.epoch());
                } else {
                  ebs_tier_storage[ip][tid] = stat.storage_consumption();
                  ebs_tier_occupancy[ip][tid] = pair<double, unsigned>(stat.occupancy(), stat.epoch());
                }
              } else if (metadata_type == "access") {
                // deserialized the value
                communication::Key_Access access;
                access.ParseFromString(res.tuple(i).value());
                if (tier_id == 1) {
                  for (int j = 0; j < access.tuple_size(); j++) {
                    memory_tier_key_access[ip][tid][access.tuple(j).key()] = access.tuple(j).access();
                    key_access_frequency[access.tuple(j).key()][ip + ":" + to_string(tid)] = access.tuple(j).access();
                  }
                } else {
                  for (int j = 0; j < access.tuple_size(); j++) {
                    ebs_tier_key_access[ip][tid][access.tuple(j).key()] = access.tuple(j).access();
                    key_access_frequency[access.tuple(j).key()][ip + ":" + to_string(tid)] = access.tuple(j).access();
                  }
                }
              }
            } else if (res.tuple(i).err_number() == 1) {
              cerr << "key " + res.tuple(i).key() + " doesn't exist\n";
            } else {
              cerr << "hash ring is inconsistent for key " + res.tuple(i).key() + "\n";
              //TODO: reach here when the hash ring is inconsistent
            }
          }
        } else {
          cerr << "request timed out\n";
          continue;
        }
      }

      unsigned long long total_memory_consumption = 0;
      unsigned long long total_ebs_consumption = 0;
      unsigned memory_node_count = 0;
      unsigned ebs_volume_count = 0;

      for (auto it1 = memory_tier_storage.begin(); it1 != memory_tier_storage.end(); it1++) {
        for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
          total_memory_consumption += it2->second;
        }
        memory_node_count += 1;
      }

      for (auto it1 = ebs_tier_storage.begin(); it1 != ebs_tier_storage.end(); it1++) {
        for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
          total_ebs_consumption += it2->second;
          ebs_volume_count += 1;
        }
      }

      if (memory_node_count != 0) {
        average_memory_consumption_new = (double)total_memory_consumption / (double)memory_node_count;
        if (average_memory_consumption != average_memory_consumption_new) {
          logger->info("avg memory consumption for epoch {} is {:03.3f}", server_monitoring_epoch, average_memory_consumption_new);
          average_memory_consumption = average_memory_consumption_new;
          for (auto it1 = memory_tier_storage.begin(); it1 != memory_tier_storage.end(); it1++) {
            for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
              logger->info("memory node ip {} thread {} consumption is {} for epoch {}", it1->first, it2->first, it2->second, server_monitoring_epoch);
            }
          }
        }
      }

      if (ebs_volume_count != 0) {
        average_ebs_consumption_new = (double)total_ebs_consumption / (double)ebs_volume_count;
        if (average_ebs_consumption != average_ebs_consumption_new) {
          logger->info("avg ebs consumption for epoch {} is {:03.3f}", server_monitoring_epoch, average_ebs_consumption_new);
          average_ebs_consumption = average_ebs_consumption_new;
        }
      }

      double max_memory_occupancy = 0.0;
      double sum_memory_occupancy = 0.0;
      unsigned count = 0;
      for (auto it1 = memory_tier_occupancy.begin(); it1 != memory_tier_occupancy.end(); it1++) {
        for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
          logger->info("memory node ip {} thread {} occupancy is {} at server epoch {} for monitoring epoch {}", it1->first, it2->first, it2->second.first, it2->second.second, server_monitoring_epoch);
          if (it2->second.first > max_memory_occupancy) {
            max_memory_occupancy = it2->second.first;
          }
          sum_memory_occupancy += it2->second.first;
          count += 1;
        }
      }
      double avg_memory_occupancy = sum_memory_occupancy / count;
      logger->info("max memory node occupancy is {}", to_string(max_memory_occupancy));
      logger->info("avg memory node occupancy is {}", to_string(avg_memory_occupancy));

      double max_ebs_occupancy = 0.0;
      double sum_ebs_occupancy = 0.0;
      count = 0;
      for (auto it1 = ebs_tier_occupancy.begin(); it1 != ebs_tier_occupancy.end(); it1++) {
        for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
          logger->info("ebs node ip {} thread {} occupancy is {} at server epoch {} for monitoring epoch {}", it1->first, it2->first, it2->second.first, it2->second.second, server_monitoring_epoch);
          if (it2->second.first > max_ebs_occupancy) {
            max_ebs_occupancy = it2->second.first;
          }
          sum_ebs_occupancy += it2->second.first;
          count += 1;
        }
      }
      double avg_ebs_occupancy = sum_ebs_occupancy / count;
      logger->info("max ebs node occupancy is {}", to_string(max_ebs_occupancy));
      logger->info("avg ebs node occupancy is {}", to_string(avg_ebs_occupancy));

      if (global_hash_ring_map[1].size() >= 2*VIRTUAL_THREAD_NUM) {
        double avg_latency = -1;
        if (user_latency.size() > 0) {
          // compute latency from users
          logger->info("computing latency from user feedback");
          double sum_latency = 0;
          unsigned count = 0;
          for (auto it = user_latency.begin(); it != user_latency.end(); it++) {
            sum_latency += it->second;
            count += 1;
          }
          avg_latency = sum_latency / count;
        } else {
          // do ping
          logger->info("pinging latency");
          unsigned ping_count = 0;
          bool succeed = true;
          auto ping_start = std::chrono::system_clock::now();
          while (ping_count < 1000) {
            string key_aux = to_string(rand() % (100000) + 1);
            string key = string(8 - key_aux.length(), '0') + key_aux;
            if (!ping(key, string(10000, 'a'), pushers, logger, mt, response_puller, global_hash_ring_map, local_hash_ring_map, placement)) {
              succeed = false;
              break;
            }
            ping_count++;
          }
          auto ping_end = std::chrono::system_clock::now();
          if (succeed) {
            avg_latency = (double)chrono::duration_cast<std::chrono::microseconds>(ping_end-ping_start).count() / ping_count;
          } else {
            logger->info("ping failed");
          }
        }
        logger->info("avg latency is {}", avg_latency);
        // policy
        auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-freeze_start).count();
        if (avg_latency != -1 && avg_latency > 1400 && adding_memory_node == 0) {
          logger->info("latency is too high");
          bool action = false;
          // first check ebs tier key access stat
          for (auto it = ebs_tier_key_access.begin(); it != ebs_tier_key_access.end(); it++) {
            string ip = it->first;
            for (auto iter = it->second.begin(); iter != it->second.end(); iter++) {
              unsigned tid = iter->first;
              for (auto key_iter = iter->second.begin(); key_iter != iter->second.end(); key_iter++) {
                string key = key_iter->first;
                if (!is_metadata(key)) {
                  logger->info("key {} accessed {} times in the last {} seconds in memory node ip {} thread {}", key, key_iter->second, SERVER_REPORT_THRESHOLD/1000000, ip, tid);
                }
              }
            }
          }
          // then check memory tier key access stat
          for (auto it = memory_tier_key_access.begin(); it != memory_tier_key_access.end(); it++) {
            string ip = it->first;
            for (auto iter = it->second.begin(); iter != it->second.end(); iter++) {
              unsigned tid = iter->first;
              for (auto key_iter = iter->second.begin(); key_iter != iter->second.end(); key_iter++) {
                string key = key_iter->first;
                if (!is_metadata(key)) {
                  logger->info("key {} accessed {} times in the last {} seconds in ebs node ip {} thread {}", key, key_iter->second, SERVER_REPORT_THRESHOLD/1000000, ip, tid);
                }
              }
            }
          }
          /*if (time_elapsed > FREEZE_PERIOD) {
            logger->info("trigger add {} memory node", to_string(NODE_ADD));
            string shell_command = "curl -X POST http://" + management_address + "/memory &";
            system(shell_command.c_str());
            adding_memory_node = NODE_ADD;
          } else {
            logger->info("freezing");
          }*/
        }
        if (avg_latency != -1 && avg_latency < 900 && !removing_memory_node && global_hash_ring_map[1].size() > 2*VIRTUAL_THREAD_NUM) {
          logger->info("latency is too low");
          /*if (time_elapsed > FREEZE_PERIOD) {
            logger->info("sending remove memory node msg");
            // pick a random memory node
            auto node = next(begin(global_hash_ring_map[1]), rand() % global_hash_ring_map[1].size())->second;
            auto connection_addr = node.get_self_depart_connect_addr();
            auto ip = node.get_ip();
            departing_node_map[ip] = tier_data_map[1].thread_number_;
            auto ack_addr = mt.get_depart_done_connect_addr();
            zmq_util::send_string(ack_addr, &pushers[connection_addr]);
            removing_memory_node = true;
          } else {
            logger->info("freezing");
          }*/
        }
      }

      /*if (average_memory_consumption >= 1000 && !adding_memory_node) {
        logger->info("trigger add memory node");
        //cerr << "trigger add memory node\n";
        string shell_command = "curl -X POST http://" + management_address + "/memory";
        system(shell_command.c_str());
        adding_memory_node = true;
      }

      if (average_ebs_consumption >= 2000 && !adding_ebs_node) {
        logger->info("trigger add ebs node");
        //cerr << "trigger add ebs node\n";
        string shell_command = "curl -X POST http://" + management_address + "/ebs";
        system(shell_command.c_str());
        adding_ebs_node = true;
      }*/
      
      report_start = std::chrono::system_clock::now();
    }
  }
}
