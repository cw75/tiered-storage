#include <zmq.hpp>
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <cstdio>
#include <pthread.h>
#include <unistd.h>
#include <memory>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <ctime>
#include "spdlog/spdlog.h"
#include "hash_ring.hpp"
#include "kvs/rc_pair_lattice.hpp"
#include "communication.pb.h"
#include "zmq/socket_cache.hpp"
#include "zmq/zmq_util.hpp"
#include "kvs/kvs_handlers.hpp"
#include "common.hpp"
#include "yaml-cpp/yaml.h"
#include "yaml-cpp/node/node.h"

using namespace std;

// define server report threshold (in second)
const unsigned SERVER_REPORT_THRESHOLD = 15;
// define server's key monitoring threshold (in second)
const unsigned KEY_MONITORING_THRESHOLD = 60;
// define the threshold for retry rep factor query for gossip handling (in second)
const unsigned RETRY_THRESHOLD = 10;

unsigned SELF_TIER_ID;
unsigned THREAD_NUM;

unsigned MEMORY_THREAD_NUM;
unsigned EBS_THREAD_NUM;

unsigned DEFAULT_GLOBAL_MEMORY_REPLICATION;
unsigned DEFAULT_GLOBAL_EBS_REPLICATION;
unsigned DEFAULT_LOCAL_REPLICATION;

// read-only per-tier metadata
unordered_map<unsigned, TierData> tier_data_map;


// thread entry point
void run(unsigned thread_id) {
  string log_file = "log_" + to_string(thread_id) + ".txt";
  string logger_name = "server_logger_" + to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  // TODO(vikram): we can probably just read this once and pass it into run
  YAML::Node conf = YAML::LoadFile("conf/config.yml")["server"];
  string ip = conf["ip"].as<string>();

  // each thread has a handle to itself
  ServerThread wt = ServerThread(ip, thread_id);

  unsigned seed = time(NULL);
  seed += thread_id;

  // prepare the zmq context
  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  // initialize hash ring maps
  unordered_map<unsigned, GlobalHashRing> global_hash_ring_map;
  unordered_map<unsigned, LocalHashRing> local_hash_ring_map;

  // for periodically redistributing data when node joins
  AddressKeysetMap join_addr_keyset_map;

  // keep track of which key should be removed when node joins
  unordered_set<string> join_remove_set;

  // pending events for asynchrony
  unordered_map<string, pair<chrono::system_clock::time_point, vector<PendingRequest>>> pending_request_map;
  unordered_map<string, pair<chrono::system_clock::time_point, vector<PendingGossip>>> pending_gossip_map;

  unordered_map<string, KeyInfo> placement;
  vector<string> routing_address;
  vector<string> monitoring_address;

  // read the YAML conf
  string seed_ip = conf["seed_ip"].as<string>();

  YAML::Node monitoring = conf["monitoring"];
  YAML::Node routing = conf["routing"];

  for (YAML::const_iterator it = routing.begin(); it != routing.end(); ++it) {
    routing_address.push_back(it->as<string>());
  }

  for (YAML::const_iterator it = monitoring.begin(); it != monitoring.end(); ++it) {
    monitoring_address.push_back(it->as<string>());
  }

  // request server addresses from the seed node
  zmq::socket_t addr_requester(context, ZMQ_REQ);
  addr_requester.connect(RoutingThread(seed_ip, 0).get_seed_connect_addr());
  zmq_util::send_string("join", &addr_requester);

  // receive and add all the addresses that seed node sent
  string serialized_addresses = zmq_util::recv_string(&addr_requester);
  communication::Address addresses;
  addresses.ParseFromString(serialized_addresses);

  // populate start time
  unsigned long long duration = addresses.start_time();
  chrono::milliseconds dur(duration);
  chrono::system_clock::time_point start_time(dur);

  // populate addresses
  for (int i = 0; i < addresses.tuple_size(); i++) {
    insert_to_hash_ring<GlobalHashRing>(global_hash_ring_map[addresses.tuple(i).tier_id()], addresses.tuple(i).ip(), 0);
  }

  // add itself to global hash ring
  insert_to_hash_ring<GlobalHashRing>(global_hash_ring_map[SELF_TIER_ID], ip, 0);

  // form local hash rings
  for (auto it = tier_data_map.begin(); it != tier_data_map.end(); it++) {
    for (unsigned tid = 0; tid < it->second.thread_number_; tid++) {
      insert_to_hash_ring<LocalHashRing>(local_hash_ring_map[it->first], ip, tid);
    }
  }

  // thread 0 notifies other servers that it has joined
  if (thread_id == 0) {
    for (auto it = global_hash_ring_map.begin(); it != global_hash_ring_map.end(); it++) {
      unsigned tier_id = it->first;
      auto hash_ring = &(it->second);
      unordered_set<string> observed_ip;

      for (auto iter = hash_ring->begin(); iter != hash_ring->end(); iter++) {
        if (iter->second.get_ip().compare(ip) != 0 && observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
          zmq_util::send_string(to_string(SELF_TIER_ID) + ":" + ip, &pushers[(iter->second).get_node_join_connect_addr()]);
          observed_ip.insert(iter->second.get_ip());
        }
      }
    }

    string msg = "join:" + to_string(SELF_TIER_ID) + ":" + ip;

    // notify proxies that this node has joined
    for (auto it = routing_address.begin(); it != routing_address.end(); it++) {
      zmq_util::send_string(msg, &pushers[RoutingThread(*it, 0).get_notify_connect_addr()]);
    }

    // notify monitoring nodes that this node has joined
    for (auto it = monitoring_address.begin(); it != monitoring_address.end(); it++) {
      zmq_util::send_string(msg, &pushers[MonitoringThread(*it).get_notify_connect_addr()]);
    }
  }

  Serializer* serializer;

  if (SELF_TIER_ID == 1) {
    MemoryKVS* kvs = new MemoryKVS();
    serializer = new MemorySerializer(kvs);
  } else if (SELF_TIER_ID == 2) {
    serializer = new EBSSerializer(thread_id);
  } else {
    logger->info("Invalid node type");
    exit(1);
  }

  // the set of changes made on this thread since the last round of gossip
  unordered_set<string> local_changeset;

  // keep track of the key stat
  unordered_map<string, KeyStat> key_stat_map;
  // keep track of key access timestamp
  unordered_map<string, multiset<std::chrono::time_point<std::chrono::system_clock>>> key_access_timestamp;
  // keep track of total access
  unsigned total_access;

  // listens for a new node joining
  zmq::socket_t join_puller(context, ZMQ_PULL);
  join_puller.bind(wt.get_node_join_bind_addr());

  // listens for a node departing
  zmq::socket_t depart_puller(context, ZMQ_PULL);
  depart_puller.bind(wt.get_node_depart_bind_addr());

  // responsible for listening for a command that this node should leave
  zmq::socket_t self_depart_puller(context, ZMQ_PULL);
  self_depart_puller.bind(wt.get_self_depart_bind_addr());

  // responsible for handling requests
  zmq::socket_t request_puller(context, ZMQ_PULL);
  request_puller.bind(wt.get_request_pulling_bind_addr());

  // responsible for processing gossips
  zmq::socket_t gossip_puller(context, ZMQ_PULL);
  gossip_puller.bind(wt.get_gossip_bind_addr());

  // responsible for listening for key replication factor response
  zmq::socket_t replication_factor_puller(context, ZMQ_PULL);
  replication_factor_puller.bind(wt.get_replication_factor_bind_addr());

  // responsible for listening for key replication factor change
  zmq::socket_t replication_factor_change_puller(context, ZMQ_PULL);
  replication_factor_change_puller.bind(wt.get_replication_factor_change_bind_addr());

  //  Initialize poll set
  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(depart_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(self_depart_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(request_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(gossip_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(replication_factor_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(replication_factor_change_puller), 0, ZMQ_POLLIN, 0 }
  };

  auto gossip_start = chrono::system_clock::now();
  auto gossip_end = chrono::system_clock::now();
  auto report_start = chrono::system_clock::now();
  auto report_end = chrono::system_clock::now();

  unsigned long long working_time = 0;
  unsigned long long working_time_map[8] = { 0, 0, 0, 0, 0, 0, 0 , 0};
  unsigned epoch = 0;

  // enter event loop
  while (true) {
    zmq_util::poll(0, &pollitems);
    // receives a node join
    if (pollitems[0].revents & ZMQ_POLLIN) {
      auto work_start = chrono::system_clock::now();

      node_join_handler(THREAD_NUM, thread_id, seed, ip, logger, &join_puller,
          global_hash_ring_map, local_hash_ring_map, key_stat_map, placement,
          join_remove_set, pushers, wt, join_addr_keyset_map);

      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[0] += time_elapsed;
    }

    if (pollitems[1].revents & ZMQ_POLLIN) {
      auto work_start = chrono::system_clock::now();

      node_depart_handler(THREAD_NUM, thread_id, ip, global_hash_ring_map, logger, &depart_puller, pushers);

      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[1] += time_elapsed;
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      auto work_start = chrono::system_clock::now();

      self_depart_handler(THREAD_NUM, thread_id, seed, ip, logger, &self_depart_puller,
          global_hash_ring_map, local_hash_ring_map, key_stat_map, placement,
          routing_address, monitoring_address, wt, pushers, serializer);

      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[2] += time_elapsed;
    }

    if (pollitems[3].revents & ZMQ_POLLIN) {
      auto work_start = chrono::system_clock::now();

      process_user_request(total_access, seed, &request_puller, start_time,
          global_hash_ring_map, local_hash_ring_map, key_stat_map,
          pending_request_map, key_access_timestamp, placement, local_changeset,
          wt, serializer, pushers);

      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[3] += time_elapsed;
    }

    // receive gossip
    if (pollitems[4].revents & ZMQ_POLLIN) {
      auto work_start = chrono::system_clock::now();

      process_gossip(seed, &gossip_puller, global_hash_ring_map, local_hash_ring_map,
          key_stat_map, pending_gossip_map, placement, wt, serializer, pushers);

      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[4] += time_elapsed;
    }

    // receives replication factor response
    if (pollitems[5].revents & ZMQ_POLLIN) {
      auto work_start = chrono::system_clock::now();

      process_rep_factor_response(seed, total_access, logger, &replication_factor_puller,
          start_time, tier_data_map, global_hash_ring_map, local_hash_ring_map,
          pending_request_map, pending_gossip_map, key_access_timestamp,
          placement, key_stat_map, local_changeset, wt, serializer, pushers);

      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[5] += time_elapsed;
    }

    // receive replication factor change
    if (pollitems[6].revents & ZMQ_POLLIN) {
      auto work_start = chrono::system_clock::now();

      process_rep_factor_change(ip, thread_id, THREAD_NUM, seed, logger, &replication_factor_change_puller, global_hash_ring_map, local_hash_ring_map, placement, key_stat_map, local_changeset, wt, serializer, pushers);

      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();
      working_time += time_elapsed;
      working_time_map[6] += time_elapsed;
    }

    // gossip updates to other threads
    gossip_end = chrono::system_clock::now();
    if (chrono::duration_cast<chrono::microseconds>(gossip_end-gossip_start).count() >= PERIOD) {
      auto work_start = chrono::system_clock::now();
      // only gossip if we have changes
      if (local_changeset.size() > 0) {
        AddressKeysetMap addr_keyset_map;

        vector<unsigned> tier_ids;
        for (unsigned i = MIN_TIER; i <= MAX_TIER; i++) {
          tier_ids.push_back(i);
        }

        bool succeed;
        for (auto it = local_changeset.begin(); it != local_changeset.end(); it++) {
          string key = *it;
          auto threads = get_responsible_threads(wt.get_replication_factor_connect_addr(), key, is_metadata(key), global_hash_ring_map, local_hash_ring_map, placement, pushers, tier_ids, succeed, seed);

          if (succeed) {
            for (auto iter = threads.begin(); iter != threads.end(); iter++) {
              if (iter->get_id() != wt.get_id()) {
                addr_keyset_map[iter->get_gossip_connect_addr()].insert(key);
              }
            }
          } else {
            logger->info("Error: missing key replication factor in gossip send routine.");
          }
        }

        send_gossip(addr_keyset_map, pushers, serializer);
        local_changeset.clear();
      }

      gossip_start = chrono::system_clock::now();
      auto time_elapsed = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-work_start).count();

      working_time += time_elapsed;
      working_time_map[7] += time_elapsed;
    }

    // collect and store internal statistics
    report_end = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::seconds>(report_end-report_start).count();

    if (duration >= SERVER_REPORT_THRESHOLD) {
      epoch += 1;
      string key = string(METADATA_IDENTIFIER) + "_" + wt.get_ip() + "_" + to_string(wt.get_tid()) + "_" + to_string(SELF_TIER_ID) + "_stat";

      // compute total storage consumption
      unsigned long long consumption = 0;
      for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
        consumption += it->second.size_;
      }

      for (int i = 0; i < sizeof(working_time_map) / sizeof(unsigned long long); i++) {
        // cast to microsecond
        double event_occupancy = (double) working_time_map[i] / ((double) duration * 1000000);

        if (event_occupancy > 0.02) {
          logger->info("Event {} occupancy is {}.", to_string(i), to_string(event_occupancy));
        }
      }

      double occupancy = (double) working_time / ((double) duration * 1000000);
      if (occupancy > 0.02) {
        logger->info("Occupancy is {}.", to_string(occupancy));
      }

      communication::Server_Stat stat;
      stat.set_storage_consumption(consumption/1000); // cast to KB
      stat.set_occupancy(occupancy);
      stat.set_epoch(epoch);
      stat.set_total_access(total_access);

      string serialized_stat;
      stat.SerializeToString(&serialized_stat);

      communication::Request req;
      req.set_type("PUT");
      prepare_put_tuple(req, key, serialized_stat, 0);

      auto threads = get_responsible_threads_metadata(key, global_hash_ring_map[1], local_hash_ring_map[1]);
      if (threads.size() != 0) {
        string target_address = next(begin(threads), rand_r(&seed) % threads.size())->get_request_pulling_connect_addr();
        push_request(req, pushers[target_address]);
      }

      // compute key access stats
      communication::Key_Access access;
      auto current_time = chrono::system_clock::now();

      for (auto it = key_access_timestamp.begin(); it != key_access_timestamp.end(); it++) {
        string key = it->first;
        auto mset = &(it->second);

        // garbage collect
        for (auto set_iter = mset->rbegin(); set_iter != mset->rend(); set_iter++) {
          if (chrono::duration_cast<std::chrono::seconds>(current_time-*set_iter).count() >= KEY_MONITORING_THRESHOLD) {
            mset->erase(mset->begin(), set_iter.base());
            break;
          }
        }

        // update key_access_frequency
        communication::Key_Access_Tuple* tp = access.add_tuple();
        tp->set_key(key);
        tp->set_access(mset->size());
      }

      // report key access stats
      key = string(METADATA_IDENTIFIER) + "_" + wt.get_ip() + "_" + to_string(wt.get_tid()) + "_" + to_string(SELF_TIER_ID) + "_access";
      string serialized_access;
      access.SerializeToString(&serialized_access);

      req.Clear();
      req.set_type("PUT");
      prepare_put_tuple(req, key, serialized_access, 0);

      threads = get_responsible_threads_metadata(key, global_hash_ring_map[1], local_hash_ring_map[1]);

      if (threads.size() != 0) {
        string target_address = next(begin(threads), rand_r(&seed) % threads.size())->get_request_pulling_connect_addr();
        push_request(req, pushers[target_address]);
      }

      report_start = chrono::system_clock::now();

      // reset stats tracked in memory
      working_time = 0;
      total_access = 0;
      memset(working_time_map, 0, sizeof(working_time_map));
    }

    // redistribute data after node joins
    if (join_addr_keyset_map.size() != 0) {
      unordered_set<string> remove_address_set;

      // assemble gossip
      AddressKeysetMap addr_keyset_map;
      for (auto it = join_addr_keyset_map.begin(); it != join_addr_keyset_map.end(); it++) {
        auto address = it->first;
        auto key_set = &(it->second);
        unsigned count = 0;

        while (count < DATA_REDISTRIBUTE_THRESHOLD && key_set->size() > 0) {
          string k = *(key_set->begin());
          addr_keyset_map[address].insert(k);

          key_set->erase(k);
          count += 1;
        }

        if (key_set->size() == 0) {
          remove_address_set.insert(address);
        }
      }

      for (auto it = remove_address_set.begin(); it != remove_address_set.end(); it++) {
        join_addr_keyset_map.erase(*it);
      }

      send_gossip(addr_keyset_map, pushers, serializer);

      // remove keys
      if (join_addr_keyset_map.size() == 0) {
        for (auto it = join_remove_set.begin(); it != join_remove_set.end(); it++) {
          key_stat_map.erase(*it);
          serializer->remove(*it);
        }
      }
    }
  }
}

int main(int argc, char* argv[]) {

  if (argc != 1) {
    cerr << "Usage: " << argv[0] << endl;
    return 1;
  }

  // populate metadata
  char* stype = getenv("SERVER_TYPE");
  if (stype != NULL) {
    SELF_TIER_ID = atoi(stype);
  } else {
    cout << "No server type specified. The default behavior is to start the server in memory mode." << endl;
    SELF_TIER_ID = 1;
  }

  YAML::Node conf = YAML::LoadFile("conf/config.yml");
  MEMORY_THREAD_NUM = conf["threads"]["memory"].as<unsigned>();
  EBS_THREAD_NUM = conf["threads"]["ebs"].as<unsigned>();

  DEFAULT_GLOBAL_MEMORY_REPLICATION = conf["replication"]["memory"].as<unsigned>();
  DEFAULT_GLOBAL_EBS_REPLICATION = conf["replication"]["ebs"].as<unsigned>();
  DEFAULT_LOCAL_REPLICATION = conf["replication"]["local"].as<unsigned>();

  tier_data_map[1] = TierData(MEMORY_THREAD_NUM, DEFAULT_GLOBAL_MEMORY_REPLICATION, MEM_NODE_CAPACITY);
  tier_data_map[2] = TierData(EBS_THREAD_NUM, DEFAULT_GLOBAL_EBS_REPLICATION, EBS_NODE_CAPACITY);

  THREAD_NUM = tier_data_map[SELF_TIER_ID].thread_number_;

  // start the initial threads based on THREAD_NUM
  vector<thread> worker_threads;
  for (unsigned thread_id = 1; thread_id < THREAD_NUM; thread_id++) {
    worker_threads.push_back(thread(run, thread_id));
  }

  run(0);
}
