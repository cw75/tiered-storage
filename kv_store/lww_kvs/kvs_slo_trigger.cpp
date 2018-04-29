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
  if (argc != 3) {
    cerr << "usage:" << argv[0] << " <slo_type> <value>" << endl;
    return 1;
  }

  string type = argv[1];
  string value = argv[2];

  if (type != "L" && type != "C") {
    cerr << "Invalid Option\n";
  }

  // read in the monitoring addresses
  string monitoring_address;
  ifstream address;
  address.open("conf/user/monitoring_address.txt");
  getline(address, monitoring_address);
  address.close();

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);
  string command = type + ":" + value;

  zmq_util::send_string(command, &pushers["tcp://" + monitoring_address + ":" + to_string(SLO_PORT)]);
}

