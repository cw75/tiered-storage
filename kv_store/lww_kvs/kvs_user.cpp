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
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"
#include "common.h"

using namespace std;

string handle_request(string input, zmq::socket_t& proxy_connector) {
  vector<string> v;
  split(input, ' ', v);

  if (v.size() == 0) {
    return "Empty request.\n";
  } else if (v[0] != "GET" && v[0] != "PUT") { 
    return "Invalid request: " + v[0] + ".\n";
  } else {
    communication::Request req;
    req.set_type(v[0]);
    req.set_metadata(false);

    vector<string> queries;
    split(v[1], ',', queries);
    for (auto it = queries.begin(); it != queries.end(); it++) {
      communication::Request_Tuple* tp = req.add_tuple();
      if (v[0] == "GET") {
        tp->set_key(*it);
      } else {
        vector<string> kv_pair;
        split(*it, ':', kv_pair);
        tp->set_key(kv_pair[0]);
        tp->set_value(kv_pair[1]);
      }
    }

    string serialized_req;
    req.SerializeToString(&serialized_req);
    zmq_util::send_string(serialized_req, &proxy_connector);

    string serialized_resp = zmq_util::recv_string(&proxy_connector);
    communication::Response resp;
    resp.ParseFromString(serialized_resp);

    // initialize the respond string
    string response_string = "";
    for (int i = 0; i < resp.tuple_size(); i++) {
      // TODO: send a more intelligent response to the user based on the response from server
      // TODO: we should send a protobuf response that is deserialized on the proxy side... allows for a programmatic API
      if (v[0] == "GET") {
        if (resp.tuple(i).succeed()) {
          response_string += ("value of key " + resp.tuple(i).key() + " is " + resp.tuple(i).value() + ".\n");
        } else {
          response_string += ("key " + resp.tuple(i).key() + " does not exist\n");
        }
      } else {
        response_string += ("succeed status is " + to_string(resp.tuple(i).succeed()) + " for key " + resp.tuple(i).key() + "\n");
      }
    }

    return response_string;
  }
}

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
  string target_proxy_address = proxy_worker_thread_t(proxy_ip, tid).request_connect_addr_;
  proxy_connector.connect(target_proxy_address);

  if (!batch) {
    string input;

    while (true) {
      cout << "kvs> ";
      getline(cin, input);
      cout << handle_request(input, proxy_connector);
    }
  } else {
    // read in the request
    string request;
    ifstream request_reader;
    request_reader.open(argv[1]);

    while (getline(request_reader, request)) {
      cout << handle_request(request, proxy_connector);
    }
  }
}
