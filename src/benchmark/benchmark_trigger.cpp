#include <stdlib.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>
#include <zmq.hpp>

#include "common.hpp"
#include "yaml-cpp/yaml.h"
#include "zmq/socket_cache.hpp"
#include "zmq/zmq_util.hpp"

unsigned DEFAULT_LOCAL_REPLICATION;

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <benchmark_threads>" << std::endl;
    return 1;
  }

  unsigned thread_num = atoi(argv[1]);
  // TODO(vikram): this is a hack that we should be able to remove once the
  // refactor is done
  DEFAULT_LOCAL_REPLICATION = 1;

  // read in the benchmark addresses
  std::vector<std::string> benchmark_address;

  // read the YAML conf
  std::vector<std::string> ips;
  YAML::Node conf = YAML::LoadFile("conf/config.yml");
  YAML::Node benchmark = conf["benchmark"];

  for (YAML::const_iterator it = benchmark.begin(); it != benchmark.end();
       ++it) {
    ips.push_back(it->as<std::string>());
  }

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  std::string command;
  while (true) {
    std::cout << "command> ";
    getline(std::cin, command);

    for (auto it = benchmark_address.begin(); it != benchmark_address.end();
         it++) {
      for (unsigned tid = 0; tid < thread_num; tid++) {
        zmq_util::send_string(
            command, &pushers["tcp://" + *it + ":" +
                              std::to_string(tid + COMMAND_BASE_PORT)]);
      }
    }
  }
}
