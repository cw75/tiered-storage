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
    size_t client_contact_port = 6560 + 600;
    // read in the initial server addresses and build the hash ring
    global_hash_t global_hash_ring;
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
	zmq::socket_t join_puller(context, ZMQ_PULL);
    // TODO: this is wonky because client_notify_bind_addr_ doesn't use the IP
    // address. should this be factored out better? why doesn't the
    // user_responder bind in this way?
    join_puller.bind(master_node_t(ip).client_notify_bind_addr_);
    // responsible for receiving user requests
    zmq::socket_t user_responder(context, ZMQ_REP);
    user_responder.bind("tcp://*:" + to_string(client_contact_port));

    string input;

    communication::Request request;

    /*zmq_pollitem_t pollitems [2];
    pollitems[0].socket = static_cast<void *>(join_puller);
    pollitems[0].events = ZMQ_POLLIN;
    pollitems[1].socket = NULL;
    pollitems[1].fd = 0;
    pollitems[1].events = ZMQ_POLLIN;*/

    vector<zmq::pollitem_t> pollitems = {
    	{ static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0 },
    	{ static_cast<void *>(user_responder), 0, ZMQ_POLLIN, 0 }
    };

    while (true) {
    	//zmq::poll(pollitems, 2, -1);
        zmq_util::poll(-1, &pollitems);
        if (pollitems[0].revents & ZMQ_POLLIN) {
            vector<string> v;
            split(zmq_util::recv_string(&join_puller), ':', v);
            if (v[0] == "join") {
            	cerr << "received join\n";
            	// update hash ring
            	global_hash_ring.insert(master_node_t(v[1]));
            	cerr << "hash ring size is " + to_string(global_hash_ring.size()) + "\n";
            }
            else if (v[0] == "depart") {
            	cerr << "received depart\n";
            	// update hash ring
            	global_hash_ring.erase(master_node_t(v[1]));
            	cerr << "hash ring size is " + to_string(global_hash_ring.size()) + "\n";
            }
        }
        else if (pollitems[1].revents & ZMQ_POLLIN) {
        	cerr << "received user request\n";
			vector<string> v; 
			split(zmq_util::recv_string(&user_responder), ' ', v);
		    if (v.size() != 0 && (v[0] == "GET" || v[0] == "PUT")) {
		    	//cerr << "hash ring size is " + to_string(global_hash_ring.size()) + "\n";
				string key = v[1];
                if (v[0] == "GET") {
                    request.mutable_get()->set_key(key);
                }
				else {
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
			            if (++it == global_hash_ring.end()) it = global_hash_ring.begin();
			        }

			        address_t server_address = server_nodes[rand()%server_nodes.size()].key_exchange_connect_addr_;
			        communication::Key_Request req;
			        req.set_sender("client");
			        communication::Key_Request_Tuple* tp = req.add_tuple();
			        tp->set_key(key);
			        string key_req;
        			req.SerializeToString(&key_req);
        			zmq_util::send_string(key_req, &cache[server_address]);
					string key_res = zmq_util::recv_string(&cache[server_address]);
					communication::Key_Response res;
					res.ParseFromString(key_res);
					//cerr << "address size is " << res.tuple(0).address_size() << "\n";
					address_t worker_address = res.tuple(0).address(0).addr();
					zmq_util::send_string(data, &cache[worker_address]);
					data = zmq_util::recv_string(&cache[worker_address]);

					communication::Response response;
					response.ParseFromString(data);

					if (v[0] == "GET") {
                        if (response.succeed())
                            zmq_util::send_string("value is " + response.value() + "\n", &user_responder);
						else
                            zmq_util::send_string("Key does not exist\n", &user_responder);
                    }
					else
                        zmq_util::send_string("succeed status is " + to_string(response.succeed()) + "\n", &user_responder);
				}
				else
                    zmq_util::send_string("no server thread available\n", &user_responder);
				request.Clear();
			}
			else {
                zmq_util::send_string("Invalid Request\n", &user_responder);
			}
        }
    }
}
