#include <zmq.hpp>
#include <string>
#include <stdlib.h>
#include <sstream>
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

// Helper function to parse user input from command line
void split(const string &s, char delim, vector<string> &elems) {
    stringstream ss(s);
    string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
}

int main(int argc, char* argv[]) {
	if (argc != 1) {
		cerr << "usage" << argv[0] << endl;
		return 1;
	}
    // init consistent hash ring
    consistent_hash_t hash_ring;
    for (int i = 0; i < THREAD_NUM; i++) {
    	hash_ring.insert(node_t("127.0.0.1", 6560 + i));
    }
    // used to hash keys
    crc32_hasher hasher;

	zmq::context_t context(1);
	SocketCache cache(&context, ZMQ_REQ);

    string input;

    communication::Request request;


	while (true) {
		cout << "Please enter a request: ";
		getline(cin, input);
		vector<string> v; 
		split(input, ' ', v);
	    if (v[0] == "GET") {
			string key = v[1];
			request.mutable_get()->set_key(key);

			string data;
			request.SerializeToString(&data);

			vector<node_t> dest_node;
			// use hash ring to find the right node to contact
			auto it = hash_ring.find(hasher(key));
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
			request.Clear();
		}
		else if (v[0] == "PUT") {
			string key = v[1];
			string value = v[2];
			request.mutable_put()->set_key(key);
			request.mutable_put()->set_value(value);
			string data;
			request.SerializeToString(&data);

			vector<node_t> dest_node;
			// use hash ring to find the right node to contact
			auto it = hash_ring.find(hasher(key));
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
			request.Clear();
		}
		else {
			cout << "Invalid Request\n";
		}
 	}
}