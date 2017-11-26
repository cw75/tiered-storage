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

  bool batch;
  if (argc == 1) {
    batch = false;
  } else if (argc == 2) {
    batch = true;
  } else {
    cerr << "invalid argument" << endl;
    return 1;
  }

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
  zmq::socket_t proxy_connector(context, ZMQ_REQ);

  // just pick the first proxy to contact for now;
  // this should eventually be round-robin / random
  string proxy_ip = *(proxy_address.begin());
  // randomly choose a proxy thread to connect
  int tid = 1 + rand() % PROXY_THREAD_NUM;
  string target_proxy_address = proxy_worker_thread_t(proxy_ip, tid).user_request_connect_addr_;
  proxy_connector.connect(target_proxy_address);

  if (!batch) {
    string input;

    while (true) {
      cout << "kvs> ";
      getline(cin, input);
      zmq_util::send_string(input, &proxy_connector);
      cout << zmq_util::recv_string(&proxy_connector);
    }
  } else {
    // read in the request
    string request;
    ifstream request_reader;
    request_reader.open(argv[1]);

    while (getline(request_reader, request)) {
      zmq_util::send_string(request, &proxy_connector);
      cout << zmq_util::recv_string(&proxy_connector);
    }
  }
}
