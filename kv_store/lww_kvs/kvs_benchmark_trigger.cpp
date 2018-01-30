#include <zmq.hpp>
#include <string>
#include <stdlib.h>
#include <sstream>
#include <fstream>
#include <vector>
#include <iostream>
#include <unistd.h>
#include <memory>
#include <unordered_set>
#include "socket_cache.h"
#include "zmq_util.h"
#include "common.h"

using namespace std;

int main(int argc, char* argv[]) {
  if (argc != 1) {
    cerr << "usage:" << argv[0] << endl;
    return 1;
  }

  // read in the benchmark addresses
  vector<string> benchmark_address;

  // read benchmark address from the file
  string ip_line;
  ifstream address;
  address.open("conf/user/benchmark_address.txt");
  while (getline(address, ip_line)) {
    benchmark_address.push_back(ip_line);
  }
  address.close();

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  string command;
  while (true) {
    cout << "command> ";
    getline(cin, command);

    for (auto it = benchmark_address.begin(); it != benchmark_address.end(); it++) {
      for (unsigned tid = 0; tid < BENCHMARK_THREAD_NUM; tid++) {
        zmq_util::send_string(command, &pushers["tcp://" + *it + ":" + to_string(tid + COMMAND_BASE_PORT)]);
      }
    }
  }
}

