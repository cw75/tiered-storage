#include <pthread.h>
#include <unistd.h>

#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <zmq.hpp>

#include "kv_store/ruc_kvs/message.pb.h"
#include "kv_store/include/rc_kv_store.h"

using namespace std;

// For simplicity, the kvs uses integer as the key type and maxintlattice as the value lattice.

// Helper function to parse user input from command line
void split(const string &s, char delim, vector<string> &elems) {
    stringstream ss(s);
    string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
}

int main ()
{
	zmq::context_t context(1);

    zmq::socket_t requester(context, ZMQ_REQ);
    requester.connect("tcp://localhost:5559");

    string input;

    int current_timestamp = -1;
	while (true) {
		//cout << static_cast<int>(getpid()) << "\n";
		communication::Request request;
		cout << "Please enter a request: ";
		getline(cin, input);
		vector<string> v;
		split(input, ' ', v);
	    if (v[0] == "BEGIN") {
	    	request.set_type("BEGIN TRANSACTION");

	    	string data;
			request.SerializeToString(&data);

            // NOTE(mwhittaker): Any reason to use the C API instead of the C++
            // API (http://api.zeromq.org/2-1:zmq-cpp)?
			zmq_msg_t msg;
			zmq_msg_init_size(&msg, data.size());
			memcpy(zmq_msg_data(&msg), &(data[0]), data.size());
			zmq_msg_send(&msg, static_cast<void *>(requester), 0);

			zmq_msg_t rec;
			zmq_msg_init(&rec);
			zmq_msg_recv(&rec, static_cast<void *>(requester), 0);
			data = (char *)zmq_msg_data(&rec);
			zmq_msg_close(&rec);
			communication::Response response;
			response.ParseFromString(data);

			current_timestamp = response.timestamp();

			cout << "timestamp is " << current_timestamp << "\n";

	    }
	    // else if (v[0] == "END TRANSACTION") {
	    // 	request.set_type("END TRANSACTION");
	    // }
		else if (v[0] == "GET") {
			//int key = stoi(v[1]);
			string key = v[1];
			request.set_type("GET");
			request.set_key(key);

			string data;
			request.SerializeToString(&data);

			zmq_msg_t msg;
			zmq_msg_init_size(&msg, data.size());
			memcpy(zmq_msg_data(&msg), &(data[0]), data.size());
			zmq_msg_send(&msg, static_cast<void *>(requester), 0);

			zmq_msg_t rec;
			zmq_msg_init(&rec);
			zmq_msg_recv(&rec, static_cast<void *>(requester), 0);
			data = (char *)zmq_msg_data(&rec);
			zmq_msg_close(&rec);
			communication::Response response;
			response.ParseFromString(data);

			cout << "value is " << response.value() << "\n";

		}
		else if (v[0] == "PUT") {
			//int key = stoi(v[1]);
			string key = v[1];
			request.set_type("PUT");
			request.set_key(key);
			request.set_value(v[2]);
			request.set_timestamp(current_timestamp);

        	string data;
			request.SerializeToString(&data);

			zmq_msg_t msg;
			zmq_msg_init_size(&msg, data.size());
			memcpy(zmq_msg_data(&msg), &(data[0]), data.size());
			zmq_msg_send(&msg, static_cast<void *>(requester), 0);

			zmq_msg_t rec;
			zmq_msg_init(&rec);
			zmq_msg_recv(&rec, static_cast<void *>(requester), 0);
			data = (char *)zmq_msg_data(&rec);
			zmq_msg_close(&rec);
			communication::Response response;
			response.ParseFromString(data);
			cout << "Successful? " << response.succeed() << "\n";
		}
		else {
			cout << "Invalid Request\n";
		}
 	}

}
