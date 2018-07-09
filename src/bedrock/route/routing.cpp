#include "hash_ring.hpp"
#include "route/routing_handlers.hpp"
#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"

// read-only per-tier metadata
std::unordered_map<unsigned, TierData> tier_data_map;
unsigned kDefaultLocalReplication;
unsigned kRoutingThreadCount;

void run(unsigned thread_id) {
  std::string log_file = "log_" + std::to_string(thread_id) + ".txt";
  std::string logger_name = "routing_logger_" + std::to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  // TODO(vikram): we can probably just read this once and pass it into run
  YAML::Node conf = YAML::LoadFile("conf/config.yml")["routing"];
  std::string ip = conf["ip"].as<std::string>();

  RoutingThread rt = RoutingThread(ip, thread_id);

  unsigned seed = time(NULL);
  seed += thread_id;

  // prepare the zmq context
  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);
  std::unordered_map<std::string, KeyInfo> placement;

  // warm up for benchmark
  // warmup_placement_to_defaults(placement);

  if (thread_id == 0) {
    // read the YAML conf
    std::vector<std::string> monitoring_address;
    YAML::Node monitoring = conf["monitoring"];

    for (YAML::const_iterator it = monitoring.begin(); it != monitoring.end();
         ++it) {
      monitoring_address.push_back(it->as<std::string>());
    }

    // notify monitoring nodes
    for (auto it = monitoring_address.begin(); it != monitoring_address.end();
         it++) {
      zmq_util::send_string(
          "join:0:" + ip,
          &pushers[MonitoringThread(*it).get_notify_connect_addr()]);
    }
  }

  // initialize hash ring maps
  std::unordered_map<unsigned, GlobalHashRing> global_hash_ring_map;
  std::unordered_map<unsigned, LocalHashRing> local_hash_ring_map;

  // pending events for asynchrony
  std::unordered_map<
      std::string, std::pair<std::chrono::system_clock::time_point,
                             std::vector<std::pair<std::string, std::string>>>>
      pending_key_request_map;

  // form local hash rings
  for (auto it = tier_data_map.begin(); it != tier_data_map.end(); it++) {
    for (unsigned tid = 0; tid < it->second.thread_number_; tid++) {
      insert_to_hash_ring<LocalHashRing>(local_hash_ring_map[it->first], ip,
                                         tid);
    }
  }

  // responsible for sending existing server addresses to a new node (relevant
  // to seed node)
  zmq::socket_t addr_responder(context, ZMQ_REP);
  addr_responder.bind(rt.get_seed_bind_addr());

  // responsible for both node join and departure
  zmq::socket_t notify_puller(context, ZMQ_PULL);
  notify_puller.bind(rt.get_notify_bind_addr());

  // responsible for listening for key replication factor response
  zmq::socket_t replication_factor_puller(context, ZMQ_PULL);
  replication_factor_puller.bind(rt.get_replication_factor_bind_addr());

  // responsible for handling key replication factor change requests from server
  // nodes
  zmq::socket_t replication_factor_change_puller(context, ZMQ_PULL);
  replication_factor_change_puller.bind(
      rt.get_replication_factor_change_bind_addr());

  // responsible for handling key address request from users
  zmq::socket_t key_address_puller(context, ZMQ_PULL);
  key_address_puller.bind(rt.get_key_address_bind_addr());

  std::vector<zmq::pollitem_t> pollitems = {
      {static_cast<void *>(addr_responder), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(notify_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(replication_factor_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(replication_factor_change_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(key_address_puller), 0, ZMQ_POLLIN, 0}};

  auto start_time = std::chrono::system_clock::now();
  auto start_time_ms =
      std::chrono::time_point_cast<std::chrono::milliseconds>(start_time);

  auto value = start_time_ms.time_since_epoch();
  unsigned long long duration = value.count();

  while (true) {
    zmq_util::poll(-1, &pollitems);

    // only relavant for the seed node
    if (pollitems[0].revents & ZMQ_POLLIN) {
      seed_handler(logger, &addr_responder, global_hash_ring_map, duration);
    }

    // handle a join or depart event coming from the server side
    if (pollitems[1].revents & ZMQ_POLLIN) {
      membership_handler(logger, &notify_puller, pushers, global_hash_ring_map,
                         thread_id, ip);
    }

    // received replication factor response
    if (pollitems[2].revents & ZMQ_POLLIN) {
      replication_response_handler(logger, &replication_factor_puller, pushers,
                                   rt, tier_data_map, global_hash_ring_map,
                                   local_hash_ring_map, placement,
                                   pending_key_request_map, seed);
    }

    if (pollitems[3].revents & ZMQ_POLLIN) {
      replication_change_handler(logger, &replication_factor_change_puller,
                                 pushers, placement, thread_id, ip);
    }

    if (pollitems[4].revents & ZMQ_POLLIN) {
      address_handler(logger, &key_address_puller, pushers, rt,
                      global_hash_ring_map, local_hash_ring_map, placement,
                      pending_key_request_map, seed);
    }
  }
}

int main(int argc, char *argv[]) {
  if (argc != 1) {
    std::cerr << "Usage: " << argv[0] << std::endl;
    return 1;
  }

  YAML::Node conf = YAML::LoadFile("conf/config.yml");
  unsigned kMemoryThreadCount = conf["threads"]["memory"].as<unsigned>();
  unsigned kEbsThreadCount = conf["threads"]["ebs"].as<unsigned>();
  kRoutingThreadCount = conf["threads"]["routing"].as<unsigned>();

  unsigned kDefaultGlobalMemoryReplication =
      conf["replication"]["memory"].as<unsigned>();
  unsigned kDefaultGlobalEbsReplication =
      conf["replication"]["ebs"].as<unsigned>();
  kDefaultLocalReplication = conf["replication"]["local"].as<unsigned>();

  tier_data_map[1] = TierData(
      kMemoryThreadCount, kDefaultGlobalMemoryReplication, kMemoryNodeCapacity);
  tier_data_map[2] = TierData(kEbsThreadCount, kDefaultGlobalEbsReplication,
                              kEbsNodeCapacity);

  std::vector<std::thread> routing_worker_threads;

  for (unsigned thread_id = 1; thread_id < kRoutingThreadCount; thread_id++) {
    routing_worker_threads.push_back(std::thread(run, thread_id));
  }

  run(0);
}
