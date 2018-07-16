//  Copyright 2018 U.C. Berkeley RISE Lab
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#include <stdlib.h>

#include "common.hpp"
#include "threads.hpp"
#include "yaml-cpp/yaml.h"

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <benchmark_threads>" << std::endl;
    return 1;
  }

  unsigned thread_num = atoi(argv[1]);

  // read in the benchmark addresses
  std::vector<Address> benchmark_address;

  // read the YAML conf
  std::vector<Address> ips;
  YAML::Node conf = YAML::LoadFile("conf/config.yml");
  YAML::Node benchmark = conf["benchmark"];

  for (const YAML::Node& node : benchmark) {
    ips.push_back(node.as<Address>());
  }

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  std::string command;
  while (true) {
    std::cout << "command> ";
    getline(std::cin, command);

    for (const std::string address : benchmark_address) {
      for (unsigned tid = 0; tid < thread_num; tid++) {
        BenchmarkThread bt = BenchmarkThread(address, tid);

        kZmqMessagingInterface->send_string(
            command, &pushers[bt.get_benchmark_command_port_addr()]);
      }
    }
  }
}
