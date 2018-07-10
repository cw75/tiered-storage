#include <pthread.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <zmq.hpp>

#include "common.hpp"
#include "communication.pb.h"
#include "hash_ring.hpp"
#include "kvs/kvs_handlers.hpp"
#include "kvs/rc_pair_lattice.hpp"
#include "spdlog/spdlog.h"
#include "yaml-cpp/node/node.h"
#include "yaml-cpp/yaml.h"
#include "zmq/socket_cache.hpp"
#include "zmq/zmq_util.hpp"

// define server report threshold (in second)
const unsigned SERVER_REPORT_THRESHOLD = 15;

// define server's key monitoring threshold (in second)
const unsigned KEY_MONITORING_THRESHOLD = 60;
// define the threshold for retry rep factor query for gossip handling (in
// second)
const unsigned RETRY_THRESHOLD = 10;
unsigned THREAD_NUM;

unsigned kSelfTierId;
std::vector<unsigned> kSelfTierIdVector;

unsigned kMemoryThreadCount;
unsigned kEbsThreadCount;

unsigned kDefaultGlobalMemoryReplication;
unsigned kDefaultGlobalEbsReplication;
unsigned kDefaultLocalReplication;

// read-only per-tier metadata
std::unordered_map<unsigned, TierData> tier_data_map;

// thread entry point
void run(unsigned thread_id) {
  std::string log_file = "log_" + std::to_string(thread_id) + ".txt";
  std::string logger_name = "server_logger_" + std::to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  // TODO(vikram): we can probably just read this once and pass it into run
  YAML::Node conf = YAML::LoadFile("conf/config.yml")["server"];
  Address ip = conf["ip"].as<Address>();

  // each thread has a handle to itself
  ServerThread wt = ServerThread(ip, thread_id);

  unsigned seed = time(NULL);
  seed += thread_id;

  // prepare the zmq context
  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  // initialize hash ring maps
  std::unordered_map<unsigned, GlobalHashRing> global_hash_ring_map;
  std::unordered_map<unsigned, LocalHashRing> local_hash_ring_map;

  // for periodically redistributing data when node joins
  AddressKeysetMap join_addr_keyset_map;

  // keep track of which key should be removed when node joins
  std::unordered_set<Key> join_remove_set;

  // pending events for asynchrony
  PendingMap<PendingRequest> pending_request_map;
  PendingMap<PendingGossip> pending_gossip_map;

  std::unordered_map<Key, KeyInfo> placement;
  std::vector<Address> routing_address;
  std::vector<Address> monitoring_address;

  // read the YAML conf
  Address seed_ip = conf["seed_ip"].as<Address>();

  YAML::Node monitoring = conf["monitoring"];
  YAML::Node routing = conf["routing"];

  for (YAML::const_iterator it = routing.begin(); it != routing.end(); ++it) {
    routing_address.push_back(it->as<Address>());
  }

  for (YAML::const_iterator it = monitoring.begin(); it != monitoring.end();
       ++it) {
    monitoring_address.push_back(it->as<Address>());
  }

  // request server addresses from the seed node
  zmq::socket_t addr_requester(context, ZMQ_REQ);
  addr_requester.connect(RoutingThread(seed_ip, 0).get_seed_connect_addr());
  zmq_util::send_string("join", &addr_requester);

  // receive and add all the addresses that seed node sent
  std::string serialized_addresses = zmq_util::recv_string(&addr_requester);
  communication::Address addresses;
  addresses.ParseFromString(serialized_addresses);

  // populate start time
  unsigned long long duration = addresses.start_time();
  std::chrono::milliseconds dur(duration);
  std::chrono::system_clock::time_point start_time(dur);

  // populate addresses
  for (int i = 0; i < addresses.tuple_size(); i++) {
    insert_to_hash_ring<GlobalHashRing>(
        global_hash_ring_map[addresses.tuple(i).tier_id()],
        addresses.tuple(i).ip(), 0);
  }

  // add itself to global hash ring
  insert_to_hash_ring<GlobalHashRing>(global_hash_ring_map[kSelfTierId], ip,
                                      0);

  // form local hash rings
  for (auto it = tier_data_map.begin(); it != tier_data_map.end(); it++) {
    for (unsigned tid = 0; tid < it->second.thread_number_; tid++) {
      insert_to_hash_ring<LocalHashRing>(local_hash_ring_map[it->first], ip,
                                         tid);
    }
  }

  // thread 0 notifies other servers that it has joined
  if (thread_id == 0) {
    for (auto it = global_hash_ring_map.begin();
         it != global_hash_ring_map.end(); it++) {
      unsigned tier_id = it->first;
      auto hash_ring = &(it->second);
      std::unordered_set<Address> observed_ip;

      for (auto iter = hash_ring->begin(); iter != hash_ring->end(); iter++) {
        if (iter->second.get_ip().compare(ip) != 0 &&
            observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
          zmq_util::send_string(
              std::to_string(kSelfTierId) + ":" + ip,
              &pushers[(iter->second).get_node_join_connect_addr()]);
          observed_ip.insert(iter->second.get_ip());
        }
      }
    }

    std::string msg = "join:" + std::to_string(kSelfTierId) + ":" + ip;

    // notify proxies that this node has joined
    for (auto it = routing_address.begin(); it != routing_address.end(); it++) {
      zmq_util::send_string(
          msg, &pushers[RoutingThread(*it, 0).get_notify_connect_addr()]);
    }

    // notify monitoring nodes that this node has joined
    for (auto it = monitoring_address.begin(); it != monitoring_address.end();
         it++) {
      zmq_util::send_string(
          msg, &pushers[MonitoringThread(*it).get_notify_connect_addr()]);
    }
  }

  Serializer *serializer;

  if (kSelfTierId == 1) {
    MemoryKVS *kvs = new MemoryKVS();
    serializer = new MemorySerializer(kvs);
  } else if (kSelfTierId == 2) {
    serializer = new EBSSerializer(thread_id);
  } else {
    logger->info("Invalid node type");
    exit(1);
  }

  // the set of changes made on this thread since the last round of gossip
  std::unordered_set<Key> local_changeset;

  // keep track of the key stat
  std::unordered_map<Key, KeyStat> key_stat_map;
  // keep track of key access timestamp
  std::unordered_map<
      Key,
      std::multiset<std::chrono::time_point<std::chrono::system_clock>>>
      key_access_timestamp;
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

  // responsible for processing gossip
  zmq::socket_t gossip_puller(context, ZMQ_PULL);
  gossip_puller.bind(wt.get_gossip_bind_addr());

  // responsible for listening for key replication factor response
  zmq::socket_t replication_factor_puller(context, ZMQ_PULL);
  replication_factor_puller.bind(wt.get_replication_factor_bind_addr());

  // responsible for listening for key replication factor change
  zmq::socket_t replication_factor_change_puller(context, ZMQ_PULL);
  replication_factor_change_puller.bind(
      wt.get_replication_factor_change_bind_addr());

  //  Initialize poll set
  std::vector<zmq::pollitem_t> pollitems = {
      {static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(depart_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(self_depart_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(request_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(gossip_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(replication_factor_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(replication_factor_change_puller), 0, ZMQ_POLLIN,
       0}};

  auto gossip_start = std::chrono::system_clock::now();
  auto gossip_end = std::chrono::system_clock::now();
  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  unsigned long long working_time = 0;
  unsigned long long working_time_map[8] = {0, 0, 0, 0, 0, 0, 0, 0};
  unsigned epoch = 0;

  // enter event loop
  while (true) {
    zmq_util::poll(0, &pollitems);
    // receives a node join
    if (pollitems[0].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      node_join_handler(THREAD_NUM, thread_id, seed, ip, logger, &join_puller,
                        global_hash_ring_map, local_hash_ring_map, key_stat_map,
                        placement, join_remove_set, pushers, wt,
                        join_addr_keyset_map);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[0] += time_elapsed;
    }

    if (pollitems[1].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      node_depart_handler(THREAD_NUM, thread_id, ip, global_hash_ring_map,
                          logger, &depart_puller, pushers);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[1] += time_elapsed;
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      self_depart_handler(
          THREAD_NUM, thread_id, seed, ip, logger, &self_depart_puller,
          global_hash_ring_map, local_hash_ring_map, key_stat_map, placement,
          routing_address, monitoring_address, wt, pushers, serializer);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[2] += time_elapsed;
    }

    if (pollitems[3].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      user_request_handler(total_access, seed, &request_puller, start_time,
                           global_hash_ring_map, local_hash_ring_map,
                           key_stat_map, pending_request_map,
                           key_access_timestamp, placement, local_changeset, wt,
                           serializer, pushers);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[3] += time_elapsed;
    }

    // receive gossip
    if (pollitems[4].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      gossip_handler(seed, &gossip_puller, global_hash_ring_map,
                     local_hash_ring_map, key_stat_map, pending_gossip_map,
                     placement, wt, serializer, pushers);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[4] += time_elapsed;
    }

    // receives replication factor response
    if (pollitems[5].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      rep_factor_response_handler(
          seed, total_access, logger, &replication_factor_puller, start_time,
          tier_data_map, global_hash_ring_map, local_hash_ring_map,
          pending_request_map, pending_gossip_map, key_access_timestamp,
          placement, key_stat_map, local_changeset, wt, serializer, pushers);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[5] += time_elapsed;
    }

    // receive replication factor change
    if (pollitems[6].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      rep_factor_change_handler(ip, thread_id, THREAD_NUM, seed, logger,
                                &replication_factor_change_puller,
                                global_hash_ring_map, local_hash_ring_map,
                                placement, key_stat_map, local_changeset, wt,
                                serializer, pushers);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[6] += time_elapsed;
    }

    // gossip updates to other threads
    gossip_end = std::chrono::system_clock::now();
    if (std::chrono::duration_cast<std::chrono::microseconds>(gossip_end -
                                                              gossip_start)
            .count() >= PERIOD) {
      auto work_start = std::chrono::system_clock::now();
      // only gossip if we have changes
      if (local_changeset.size() > 0) {
        AddressKeysetMap addr_keyset_map;

        bool succeed;
        for (auto it = local_changeset.begin(); it != local_changeset.end();
             it++) {
          Key key = *it;
          auto threads = get_responsible_threads(
              wt.get_replication_factor_connect_addr(), key, is_metadata(key),
              global_hash_ring_map, local_hash_ring_map, placement, pushers,
              kAllTierIds, succeed, seed);

          if (succeed) {
            for (auto iter = threads.begin(); iter != threads.end(); iter++) {
              if (iter->get_id() != wt.get_id()) {
                addr_keyset_map[iter->get_gossip_connect_addr()].insert(key);
              }
            }
          } else {
            logger->info(
                "Error: missing key replication factor in gossip send "
                "routine.");
          }
        }

        send_gossip(addr_keyset_map, pushers, serializer);
        local_changeset.clear();
      }

      gossip_start = std::chrono::system_clock::now();
      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();

      working_time += time_elapsed;
      working_time_map[7] += time_elapsed;
    }

    // collect and store internal statistics
    report_end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                        report_end - report_start)
                        .count();

    if (duration >= SERVER_REPORT_THRESHOLD) {
      epoch += 1;
      Key key = std::string(kMetadataIdentifier) + "_" + wt.get_ip() +
                        "_" + std::to_string(wt.get_tid()) + "_" +
                        std::to_string(kSelfTierId) + "_stat";

      // compute total storage consumption
      unsigned long long consumption = 0;
      for (auto it = key_stat_map.begin(); it != key_stat_map.end(); it++) {
        consumption += it->second.size_;
      }

      for (int i = 0; i < sizeof(working_time_map) / sizeof(unsigned long long);
           i++) {
        // cast to microsecond
        double event_occupancy =
            (double)working_time_map[i] / ((double)duration * 1000000);

        if (event_occupancy > 0.02) {
          logger->info("Event {} occupancy is {}.", std::to_string(i),
                       std::to_string(event_occupancy));
        }
      }

      double occupancy = (double)working_time / ((double)duration * 1000000);
      if (occupancy > 0.02) {
        logger->info("Occupancy is {}.", std::to_string(occupancy));
      }

      communication::Server_Stat stat;
      stat.set_storage_consumption(consumption / 1000);  // cast to KB
      stat.set_occupancy(occupancy);
      stat.set_epoch(epoch);
      stat.set_total_access(total_access);

      std::string serialized_stat;
      stat.SerializeToString(&serialized_stat);

      communication::Request req;
      req.set_type("PUT");
      prepare_put_tuple(req, key, serialized_stat, 0);

      auto threads = get_responsible_threads_metadata(
          key, global_hash_ring_map[1], local_hash_ring_map[1]);
      if (threads.size() != 0) {
        Address target_address =
            next(begin(threads), rand_r(&seed) % threads.size())
                ->get_request_pulling_connect_addr();
        push_request(req, pushers[target_address]);
      }

      // compute key access stats
      communication::Key_Access access;
      auto current_time = std::chrono::system_clock::now();

      for (auto it = key_access_timestamp.begin();
           it != key_access_timestamp.end(); it++) {
        Key key = it->first;
        auto mset = &(it->second);

        // garbage collect
        for (auto set_iter = mset->rbegin(); set_iter != mset->rend();
             set_iter++) {
          if (std::chrono::duration_cast<std::chrono::seconds>(current_time -
                                                               *set_iter)
                  .count() >= KEY_MONITORING_THRESHOLD) {
            mset->erase(mset->begin(), set_iter.base());
            break;
          }
        }

        // update key_access_frequency
        communication::Key_Access_Tuple *tp = access.add_tuple();
        tp->set_key(key);
        tp->set_access(mset->size());
      }

      // report key access stats
      key = std::string(kMetadataIdentifier) + "_" + wt.get_ip() + "_" +
            std::to_string(wt.get_tid()) + "_" + std::to_string(kSelfTierId) +
            "_access";
      std::string serialized_access;
      access.SerializeToString(&serialized_access);

      req.Clear();
      req.set_type("PUT");
      prepare_put_tuple(req, key, serialized_access, 0);

      threads = get_responsible_threads_metadata(key, global_hash_ring_map[1],
                                                 local_hash_ring_map[1]);

      if (threads.size() != 0) {
        Address target_address =
            next(begin(threads), rand_r(&seed) % threads.size())
                ->get_request_pulling_connect_addr();
        push_request(req, pushers[target_address]);
      }

      report_start = std::chrono::system_clock::now();

      // reset stats tracked in memory
      working_time = 0;
      total_access = 0;
      memset(working_time_map, 0, sizeof(working_time_map));
    }

    // redistribute data after node joins
    if (join_addr_keyset_map.size() != 0) {
      std::unordered_set<Address> remove_address_set;

      // assemble gossip
      AddressKeysetMap addr_keyset_map;
      for (auto it = join_addr_keyset_map.begin();
           it != join_addr_keyset_map.end(); it++) {
        auto address = it->first;
        auto key_set = &(it->second);
        unsigned count = 0;

        while (count < DATA_REDISTRIBUTE_THRESHOLD && key_set->size() > 0) {
          Key k = *(key_set->begin());
          addr_keyset_map[address].insert(k);

          key_set->erase(k);
          count += 1;
        }

        if (key_set->size() == 0) {
          remove_address_set.insert(address);
        }
      }

      for (auto it = remove_address_set.begin(); it != remove_address_set.end();
           it++) {
        join_addr_keyset_map.erase(*it);
      }

      send_gossip(addr_keyset_map, pushers, serializer);

      // remove keys
      if (join_addr_keyset_map.size() == 0) {
        for (auto it = join_remove_set.begin(); it != join_remove_set.end();
             it++) {
          key_stat_map.erase(*it);
          serializer->remove(*it);
        }
      }
    }
  }
}

int main(int argc, char *argv[]) {
  if (argc != 1) {
    std::cerr << "Usage: " << argv[0] << std::endl;
    return 1;
  }

  // populate metadata
  char *stype = getenv("SERVER_TYPE");
  if (stype != NULL) {
    kSelfTierId = atoi(stype);
  } else {
    std::cout
        << "No server type specified. The default behavior is to start the "
           "server in memory mode."
        << std::endl;
    kSelfTierId = 1;
  }

  kSelfTierIdVector = { kSelfTierId };

  YAML::Node conf = YAML::LoadFile("conf/config.yml");
  kMemoryThreadCount = conf["threads"]["memory"].as<unsigned>();
  kEbsThreadCount = conf["threads"]["ebs"].as<unsigned>();

  kDefaultGlobalMemoryReplication =
      conf["replication"]["memory"].as<unsigned>();
  kDefaultGlobalEbsReplication = conf["replication"]["ebs"].as<unsigned>();
  kDefaultLocalReplication = conf["replication"]["local"].as<unsigned>();

  tier_data_map[1] = TierData(
      kMemoryThreadCount, kDefaultGlobalMemoryReplication, kMemoryNodeCapacity);
  tier_data_map[2] = TierData(kEbsThreadCount, kDefaultGlobalEbsReplication,
                              kEbsNodeCapacity);

  THREAD_NUM = tier_data_map[kSelfTierId].thread_number_;

  // start the initial threads based on THREAD_NUM
  std::vector<std::thread> worker_threads;
  for (unsigned thread_id = 1; thread_id < THREAD_NUM; thread_id++) {
    worker_threads.push_back(std::thread(run, thread_id));
  }

  run(0);
}
