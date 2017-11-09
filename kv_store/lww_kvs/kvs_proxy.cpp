#include <zmq.hpp>
#include <string>
#include <stdlib.h>
#include <sstream>
#include <fstream>
#include <vector>
#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include <memory>
#include "rc_kv_store.h"
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"
#include "consistent_hash_map.hpp"
#include "common.h"

using namespace std;
using address_t = string;

#define DEFAULT_GLOBAL_MEMORY_REPLICATION 1
#define DEFAULT_GLOBAL_EBS_REPLICATION 2
#define DEFAULT_LOCAL_EBS_REPLICATION 1

// TODO: instead of cout or cerr, everything should be written to a log file.
int main(int argc, char* argv[]) {
  if (argc != 1) {
    cerr << "usage:" << argv[0] << endl;
    return 1;
  }

  string ip = getIP();

  global_hash_t global_memory_hash_ring;
  global_hash_t global_ebs_hash_ring;

  // keep track of the keys' replication info
  unordered_map<string, key_info> placement;
 
  // read in the initial server addresses and build the hash ring
  string ip_line;
  ifstream address;
  // read existing memory servers and populate the memory hash ring
  address.open("conf/client/existing_memory_servers.txt");

  while (getline(address, ip_line)) {
    cerr << ip_line << "\n";
    global_memory_hash_ring.insert(master_node_t(ip_line, "M"));
  }
  address.close();

  // read existing ebs servers and populate the ebs hash ring
  address.open("conf/client/existing_ebs_servers.txt");

  while (getline(address, ip_line)) {
    cerr << ip_line << "\n";
    global_ebs_hash_ring.insert(master_node_t(ip_line, "E"));
  }
  address.close();


  zmq::context_t context(1);
  SocketCache cache(&context, ZMQ_REQ);

  // responsible for both node join and departure
  zmq::socket_t join_puller(context, ZMQ_PULL);
  join_puller.bind(CLIENT_NOTIFY_BIND_ADDR);
  // responsible for receiving user requests
  zmq::socket_t user_responder(context, ZMQ_REP);
  user_responder.bind(CLIENT_CONTACT_BIND_ADDR);

  string input;
  communication::Request request;
  communication::Key_Request server_req;
  communication::Key_Response res;
  communication::Response response;

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(user_responder), 0, ZMQ_POLLIN, 0 }
  };

  while (true) {
    // listen for ZMQ events
    zmq_util::poll(-1, &pollitems);

    // handle a join or depart event coming from the server side
    if (pollitems[0].revents & ZMQ_POLLIN) {
      vector<string> v;
      split(zmq_util::recv_string(&join_puller), ':', v);
      if (v[0] == "join") {
        cerr << "received join\n";
        // update hash ring
        if (v[1] == "M") {
          global_memory_hash_ring.insert(master_node_t(v[2], "M"));
        } else if (v[1] == "E") {
          global_ebs_hash_ring.insert(master_node_t(v[2], "E"));
        } else {
          cerr << "Invalid Tier info\n";
        }
        cerr << "memory hash ring size is " + to_string(global_memory_hash_ring.size()) + "\n";
        cerr << "ebs hash ring size is " + to_string(global_ebs_hash_ring.size()) + "\n";
      } else if (v[0] == "depart") {
        cerr << "received depart\n";
        // update hash ring
        if (v[1] == "M") {
          global_memory_hash_ring.erase(master_node_t(v[2], "M"));
        } else if (v[1] == "E") {
          global_ebs_hash_ring.erase(master_node_t(v[2], "E"));
        } else {
          cerr << "Invalid Tier info\n";
        }
        cerr << "memory hash ring size is " + to_string(global_memory_hash_ring.size()) + "\n";
        cerr << "ebs hash ring size is " + to_string(global_ebs_hash_ring.size()) + "\n";
      }
    } else if (pollitems[1].revents & ZMQ_POLLIN) {
      // handle a user facing request
      cerr << "received user request\n";
      vector<string> v; 

      // NOTE: once we start thinking about building a programmatic API, we
      // will need a more robust form of serialization between the user & the
      // proxy & the server
      split(zmq_util::recv_string(&user_responder), ' ', v);

      if (v.size() == 0) {
          zmq_util::send_string("Empty request.\n", &user_responder);
      } else if (v[0] != "GET" && v[0] != "PUT") { 
          zmq_util::send_string("Invalid request: " + v[0] + ".\n", &user_responder);
      } else {
        string key = v[1];
        // set the key info for this key (using the default replication factor for now)
        placement[key] = key_info(DEFAULT_GLOBAL_MEMORY_REPLICATION, DEFAULT_GLOBAL_EBS_REPLICATION, DEFAULT_LOCAL_EBS_REPLICATION);
        if (v[0] == "GET") {
          request.mutable_get()->set_key(key);
        } else { // i.e., the request is a PUT
          request.mutable_put()->set_key(key);
          request.mutable_put()->set_value(v[2]);
        }

        string data;
        request.SerializeToString(&data);

        vector<master_node_t> server_nodes;
        // use hash ring to find the right node to contact
        // first, look up the memory hash ring
        auto it = global_memory_hash_ring.find(key);
        if (it != global_memory_hash_ring.end()) {
          for (int i = 0; i < placement[key].global_memory_replication_; i++) {
            server_nodes.push_back(it->second);
            if (++it == global_memory_hash_ring.end()) {
              it = global_memory_hash_ring.begin();
            }
          }
        }
        // then check the ebs hash ring
        it = global_ebs_hash_ring.find(key);
        if (it != global_ebs_hash_ring.end()) {
          for (int i = 0; i < placement[key].global_ebs_replication_; i++) {
            server_nodes.push_back(it->second);
            if (++it == global_ebs_hash_ring.end()) {
              it = global_ebs_hash_ring.begin();
            }
          }
        }

        if (server_nodes.size() != 0) {
          // get the address-port combination for a particular server; which
          // server the request is sent to is chosen at random
          master_node_t server_node = server_nodes[rand() % server_nodes.size()];
          address_t server_address = server_node.key_exchange_connect_addr_;
          string server_tier = server_node.tier_;
          // create a request and set the tuple to have the key we want
          server_req.set_sender("client");
          communication::Key_Request_Tuple* tp = server_req.add_tuple();
          tp->set_key(key);
          tp->set_global_memory_replication(placement[key].global_memory_replication_);
          tp->set_global_ebs_replication(placement[key].global_ebs_replication_);
          tp->set_local_ebs_replication(placement[key].local_ebs_replication_);

          // serialize request and send
          string key_req;
          server_req.SerializeToString(&key_req);
          zmq_util::send_string(key_req, &cache[server_address]);

          address_t worker_address;
          // wait for a response from the server and deserialize
          if (server_tier == "E") {
            string key_res = zmq_util::recv_string(&cache[server_address]);
            res.ParseFromString(key_res);

            // get the worker address from the response and sent the serialized
            // data from up above to the worker thread; the reason that we do
            // this is to let the metadata thread avoid having to receive a
            // potentially large request body; since the metadata thread is
            // serial, this could potentially be a bottleneck; the current way
            // allows the metadata thread to answer lightweight requests only
            //
            // TODO: currently we only pick a random worker; we should allow
            // requests with multiple keys in the future
            worker_address =  res.tuple(0).address(rand() % res.tuple(0).address().size()).addr();
          } else {
            worker_address =  zmq_util::recv_string(&cache[server_address]);
          }
          cout << "worker address is " + worker_address + "\n";
          zmq_util::send_string(data, &cache[worker_address]);          

          // wait for response to actual request
          data = zmq_util::recv_string(&cache[worker_address]);
          response.ParseFromString(data);
          

          // based on the request type and response content, send a message
          // back to the user
          // TODO: send a more intelligent response to the user based on the response from server
          // TODO: we should send a protobuf response that is deserialized on the client side... allows for a programmatic API
          if (v[0] == "GET") {
            if (response.succeed()) {
              zmq_util::send_string("Value is " + response.value() + ".\n", &user_responder);
            } else {
              zmq_util::send_string("Key does not exist\n", &user_responder);
            }
          } else {
            zmq_util::send_string("succeed status is " + to_string(response.succeed()) + "\n", &user_responder);
          }
        } else {
          zmq_util::send_string("No servers available.\n", &user_responder);
        }
        
        request.Clear();
        server_req.Clear();
        res.Clear();
        response.Clear();
      }     
    }
  }
}
