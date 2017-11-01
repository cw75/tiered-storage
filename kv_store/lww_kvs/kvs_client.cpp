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

// TODO: instead of cout or cerr, everything should be written to a log file.
int main(int argc, char* argv[]) {
  string ip = getIP();
  size_t client_contact_port = SERVER_PORT + CLIENT_USER_OFFSET;

  global_hash_t global_hash_ring;
 
  // read in the initial server addresses and build the hash ring
  string ip_line;
  ifstream address;
  address.open("conf/client/existing_servers.txt");

  while (getline(address, ip_line)) {
    cerr << ip_line << "\n";
    global_hash_ring.insert(master_node_t(ip_line));
  }
  address.close();

  zmq::context_t context(1);
  SocketCache cache(&context, ZMQ_REQ);

  // responsible for both node join and departure
  // TODO: this is wonky because client_notify_bind_addr_ doesn't use the IP
  // address. should this be factored out better? why doesn't the
  // user_responder bind in this way on lin 50?
  zmq::socket_t join_puller(context, ZMQ_PULL);
  join_puller.bind(master_node_t(ip).client_notify_bind_addr_);
  // responsible for receiving user requests
  zmq::socket_t user_responder(context, ZMQ_REP);
  user_responder.bind("tcp://*:" + to_string(client_contact_port));

  string input;
  // TODO: why are we reusing the same request again and again here but not 
  // for other kinds of requests below?
  communication::Request request;

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(user_responder), 0, ZMQ_POLLIN, 0 }
  };

  while (true) {
    // listen for ZMQ events
    // TODO: does this mean that we can only ever handle a single user vs.
    // server facing request at a time? That's what it seems like to me, but
    // I'm not sure if I'm missing something related to threading.
    zmq_util::poll(-1, &pollitems);

    // handle a join or depart event coming from the server side
    if (pollitems[0].revents & ZMQ_POLLIN) {
      vector<string> v;
      split(zmq_util::recv_string(&join_puller), ':', v);
      if (v[0] == "join") {
        cerr << "received join\n";
        // update hash ring
        global_hash_ring.insert(master_node_t(v[1]));
        cerr << "hash ring size is " + to_string(global_hash_ring.size()) + "\n";
      } else if (v[0] == "depart") {
        cerr << "received depart\n";
        // update hash ring
        global_hash_ring.erase(master_node_t(v[1]));
        cerr << "hash ring size is " + to_string(global_hash_ring.size()) + "\n";
      }
    } else if (pollitems[1].revents & ZMQ_POLLIN) {
      // handle a user facing request
      cerr << "received user request\n";
      vector<string> v; 

      // TODO: there should be a programmatic API as well, so we probably don't
      // want to just rely on splitting by spaces?
      split(zmq_util::recv_string(&user_responder), ' ', v);

      // TODO: why are we checking if v.size() is 0? does that just mean that
      // we got an empty request; shouldn't we require the size to be >= 2
      // specifically? doesn't make sense to process just a "GET" with no key?
      if (v.size() != 0 && (v[0] == "GET" || v[0] == "PUT")) {
        string key = v[1];
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
        auto it = global_hash_ring.find(key);
        if (it != global_hash_ring.end()) {
          for (int i = 0; i < GLOBAL_EBS_REPLICATION; i++) {
            server_nodes.push_back(it->second);
            if (++it == global_hash_ring.end()) {
              it = global_hash_ring.begin();
            }
          }

          // get the address-port combination for a particular server; which
          // server the request is sent to is chosen at random
          address_t server_address = server_nodes[rand() % server_nodes.size()].key_exchange_connect_addr_;

          // create a request and set the tuple to have the key we want
          communication::Key_Request server_req;
          server_req.set_sender("client");
          communication::Key_Request_Tuple* tp = server_req.add_tuple();
          tp->set_key(key);

          // serialize request and send
          string key_req;
          server_req.SerializeToString(&key_req);
          zmq_util::send_string(key_req, &cache[server_address]);

          // wait for a response from the server and deserialize
          // TODO: is this synchronous? shouldn't it be async?
          string key_res = zmq_util::recv_string(&cache[server_address]);
          communication::Key_Response res;
          res.ParseFromString(key_res);

          // get the worker address from the response and sent the serialized
          // data from up above to the worker thread
          // TODO: why do we need to do this? why can't the metadata thread
          // automatically route a request based on the key to the correct
          // thread locally via ZMQ? this seems like unnecessary communication
          address_t worker_address = res.tuple(0).address(0).addr();
          zmq_util::send_string(data, &cache[worker_address]);

          // wait for response to actual request
          data = zmq_util::recv_string(&cache[worker_address]);
          communication::Response response;
          response.ParseFromString(data);

          // based on the request type and response content, send a message
          // back to the user
          if (v[0] == "GET") {
            if (response.succeed()) {
              zmq_util::send_string("value is " + response.value() + "\n", &user_responder);
            } else {
              zmq_util::send_string("Key does not exist\n", &user_responder);
            }
          } else {
            zmq_util::send_string("succeed status is " + to_string(response.succeed()) + "\n", &user_responder);
          }
        } else {
          // TODO: this doesn't make sense to me... if global_hash_ring.find
          // doesn't return any nodes, why are no server *threads* available?
          // doesn't that just mean that there are no servers available at all?
          zmq_util::send_string("no server thread available\n", &user_responder);
        }
        
        request.Clear();
      } else {
        if (v.size() == 0) {
          zmq_util::send_string("Empty request.\n", &user_responder);
        } else {
          zmq_util::send_string("Invalid request: " + v[0] + ".\n", &user_responder);
        }
      }
    }
  }
}
