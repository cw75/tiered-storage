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

using namespace std;

int main(int argc, char* argv[]) {
    size_t client_contact_port = 6560 + 600;
    // read in the client addresses
    unordered_set<string> client_address;

    // read client address from the file
    string ip_line;
    ifstream address;
    address.open("build/kv_store/lww_kvs/client_address.txt");
    while (getline(address, ip_line)) {
        client_address.insert(ip_line);
    }
    address.close();

    // just pick the first client proxy to contact for now
    string client_ip = *(client_address.begin());

	zmq::context_t context(1);
    zmq::socket_t client_connector(context, ZMQ_REQ);
    client_connector.connect("tcp://" + client_ip + ":" + to_string(client_contact_port));

    string input;

    while (true) {
        getline(cin, input);
        zmq_util::send_string(input, &client_connector);
        cout << zmq_util::recv_string(&client_connector);
    }
}
