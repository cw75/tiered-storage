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

typedef consistent_hash_map<node_t,crc32_hasher> consistent_hash_t;

int main(int argc, char* argv[]) {
    if (argc != 2) {
        cerr << "usage:" << argv[0] << " <client_address>" << endl;
        return 1;
    }
    string ip = argv[1];
    size_t port = 6560;
    // read in the initial server addresses and build the hash ring
    consistent_hash_t hash_ring;
    string ip_line;
    ifstream address;
    address.open("/home/ubuntu/research/high-performance-lattices/kv_store/lww_kvs/server_address.txt");
    while (getline(address, ip_line)) {
        cout << ip_line << "\n";
        for (int i = 1; i <= THREAD_NUM; i++) {
            hash_ring.insert(node_t(ip_line, 6560 + i));
        }
    }
    address.close();
    // used to hash keys
    crc32_hasher hasher;

	zmq::context_t context(1);
	SocketCache cache(&context, ZMQ_REQ);

	zmq::socket_t join_puller(context, ZMQ_PULL);
    join_puller.bind("tcp://*:" + to_string(port + 500));

    string input;

    communication::Request request;

    zmq_pollitem_t pollitems [2];
    pollitems[0].socket = static_cast<void *>(join_puller);
    pollitems[0].events = ZMQ_POLLIN;
    pollitems[1].socket = NULL;
    pollitems[1].fd = 0;
    pollitems[1].events = ZMQ_POLLIN;

    /*vector<zmq::pollitem_t> pollitems = {
    	//{ NULL, 0, ZMQ_POLLIN, 0 },
    	{ static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0 }
    };*/

    while (true) {
    	zmq::poll(pollitems, 2, -1);
        //zmq_util::poll(0, &pollitems);
        if (pollitems[0].revents & ZMQ_POLLIN) {
            vector<string> v;
            split(zmq_util::recv_string(&join_puller), ':', v);
            if (v[0] == "join") {
            	cout << "received join\n";
            	// update hash ring
            	hash_ring.insert(node_t(v[1], stoi(v[2])));
            	cout << "hash ring size is " + to_string(hash_ring.size()) + "\n";
            }
            else if (v[0] == "depart") {
            	cout << "received depart\n";
            	// update hash ring
            	hash_ring.erase(node_t(v[1], stoi(v[2])));
            	cout << "hash ring size is " + to_string(hash_ring.size()) + "\n";
            }
        }
        else {
        	//cout << "received something\n";
			getline(cin, input);
			//cout << input << "\n";
			vector<string> v; 
			split(input, ' ', v);
		    if (v.size() != 0 && v[0] == "GET") {
		    	//cout << "hash ring size is " + to_string(hash_ring.size()) + "\n";
				string key = v[1];
				request.mutable_get()->set_key(key);

				string data;
				request.SerializeToString(&data);

				vector<node_t> dest_node;
				// use hash ring to find the right node to contact
				auto it = hash_ring.find(hasher(key));
				if (it != hash_ring.end()) {
					for (int i = 0; i < REPLICATION; i++) {
		                dest_node.push_back(it->second);
			            if (++it == hash_ring.end()) it = hash_ring.begin();
			        }

			        address_t dest_addr = dest_node[rand()%dest_node.size()].client_connection_addr_;

					zmq_util::send_string(data, &cache[dest_addr]);
					data = zmq_util::recv_string(&cache[dest_addr]);

					communication::Response response;
					response.ParseFromString(data);

					cout << "value is " << response.value() << "\n";
				}
				else cout << "no server thread available\n";
				request.Clear();
			}
			else if (v.size() != 0 && v[0] == "PUT") {
				//cout << "hash ring size is " + to_string(hash_ring.size()) + "\n";
				string key = v[1];
				string value = v[2];
				request.mutable_put()->set_key(key);
				request.mutable_put()->set_value(value);
				string data;
				request.SerializeToString(&data);

				vector<node_t> dest_node;
				// use hash ring to find the right node to contact
				auto it = hash_ring.find(hasher(key));
				if (it != hash_ring.end()) {
					for (int i = 0; i < REPLICATION; i++) {
		                dest_node.push_back(it->second);
			            if (++it == hash_ring.end()) it = hash_ring.begin();
			        }

			        address_t dest_addr = dest_node[rand()%dest_node.size()].client_connection_addr_;
					
					zmq_util::send_string(data, &cache[dest_addr]);
					data = zmq_util::recv_string(&cache[dest_addr]);

					communication::Response response;
					response.ParseFromString(data);

					cout << "succeed status is " << response.succeed() << "\n";					
				}
				else cout << "no server thread available\n";
				request.Clear();
			}
			else {
				cout << "Invalid Request\n";
			}
        }
    }
}
