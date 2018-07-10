#include <stdlib.h>

#include "common.hpp"
#include "yaml-cpp/yaml.h"

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <benchmark_threads>" << std::endl;
    return 1;
  }

  unsigned thread_num = atoi(argv[1]);

  // read in the benchmark addresses
  std::vector<std::string> benchmark_address;

  // read the YAML conf
  std::vector<std::string> ips;
  YAML::Node conf = YAML::LoadFile("conf/config.yml");
  YAML::Node benchmark = conf["benchmark"];

  for (const YAML::Node& node : benchmark) {
    ips.push_back(node.as<std::string>());
  }

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  std::string command;
  while (true) {
    std::cout << "command> ";
    getline(std::cin, command);

    for (const std::string address : benchmark_address) {
      for (unsigned tid = 0; tid < thread_num; tid++) {
        zmq_util::send_string(
            command, &pushers["tcp://" + address + ":" +
                              std::to_string(tid + kBenchmarkCommandBasePort)]);
      }
    }
  }
}
