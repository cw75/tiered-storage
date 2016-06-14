#include <zmq.hpp>
#include <string>
#include <sstream>
#include <vector>
#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include <memory>
#include "kv_store.h"
#include "request.pb.h"
#include "response.pb.h"

using namespace std;

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

   //  communication::Request req;
   //  req.set_type("GET");
  	// req.set_key(1);
  	// string data;
  	// req.SerializeToString(&data);

  	// zmq_msg_t msg;
   //  zmq_msg_init_size(&msg, data.size());
   //  memcpy(zmq_msg_data(&msg), &(data[0]), data.size());
   //  zmq_msg_send(&msg, static_cast<void *>(requester), 0);

   //  zmq_msg_t rec;
   //  zmq_msg_init(&rec);
   //  zmq_msg_recv(&rec, static_cast<void *>(requester), 0);
   //  data = (char *)zmq_msg_data(&rec);
   //  zmq_msg_close(&rec);
   //  communication::Response response;
   //  response.ParseFromString(data);

   //  cout << "error ?: " << response.err() << "\n";
   //  cout << "value is " << response.value() << "\n";

    string input;
    unordered_map<int, unordered_map<int, int>> version_map;
	while (true) {
		//cout << static_cast<int>(getpid()) << "\n";
		communication::Request request;
		cout << "Please enter a request: ";
		getline(cin, input);
		vector<string> v;
		split(input, ' ', v);
		if (v[0] == "GET") {
			int key = stoi(v[1]);
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
			unordered_map<int, int> tmp;
			auto m = response.version_vector();
			for (auto it = m.begin(); it != m.end(); ++it) {
				tmp.emplace(it->first, it->second);
			}
			version_map[key] = tmp;
			cout << "value is " << response.value() << "\n";

		}
		else if (v[0] == "PUT") {
			int key = stoi(v[1]);
			request.set_type("PUT");
			request.set_key(key);
			request.set_value(stoi(v[2]));
			auto it = version_map[key].find(getpid());
			cout << "version id is " << version_map[key][getpid()] << "\n";
			if (it == version_map[key].end()) {
				version_map[key][getpid()] = 1;
			}
			else version_map[key][getpid()]++;
			for (auto it = version_map[key].begin(); it != version_map[key].end(); ++it) {
            	(*request.mutable_version_vector())[it->first] = it->second;
        	}
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

// string prepare_request(vector<string> &v) {
// 	if (v[0] == "GET") {
// 		request.set_type("GET");
// 		request.set_key(stoi(v[1]));
// 	}
// 	else {
// 		request.set_type("PUT");
// 		request.set_key(stoi(v[1]));
// 		request.set_value(stoi(v[2]));
// 	}
// }