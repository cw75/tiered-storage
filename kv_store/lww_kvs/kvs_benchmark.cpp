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

  // read in the proxy addresses
  vector<string> proxy_address;

  // read proxy address from the file
  string ip_line;
  ifstream address;
  address.open("conf/user/proxy_address.txt");
  while (getline(address, ip_line)) {
    proxy_address.push_back(ip_line);
  }
  address.close();

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  string input;

  while (true) {
    cout << "benchmark> ";
    getline(cin, input);

    for (auto it = proxy_address.begin(); it != proxy_address.end(); it++) {
      for (int tid = 1; tid <= PROXY_THREAD_NUM; tid++) {
        zmq_util::send_string(input, &pushers[proxy_worker_thread_t(*it, tid).banchmark_connect_addr_]);
      }
    }
  }
}