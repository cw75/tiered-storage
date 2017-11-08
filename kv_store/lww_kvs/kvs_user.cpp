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
  // read in the client addresses
  unordered_set<string> client_address;

  // read client address from the file
  string ip_line;
  ifstream address;
  address.open("conf/user/client_address.txt");
  while (getline(address, ip_line)) {
    client_address.insert(ip_line);
  }
  address.close();

  // just pick the first client proxy to contact for now;
  // this should eventually be round-robin / random
  string client_ip = *(client_address.begin());

  zmq::context_t context(1);
  zmq::socket_t client_connector(context, ZMQ_REQ);
  client_connector.connect("tcp://" + client_ip + ":" + to_string(CLIENT_USER_PORT));

  string input;

  while (true) {
    cout << "kvs> ";
    getline(cin, input);
    zmq_util::send_string(input, &client_connector);
    cout << zmq_util::recv_string(&client_connector);
  }
}
