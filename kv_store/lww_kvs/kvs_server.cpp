#include <zmq.hpp>
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <pthread.h>
#include <unistd.h>
#include <memory>
#include <vector>
#include <thread>
#include "rc_kv_store.h"
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"
#include "consistent_hash_map.hpp"
#include "common.h"

using namespace std;

// If the total number of updates to the kvs before the last gossip reaches THRESHOLD, then the thread gossips to others.
#define THRESHOLD 1

// For simplicity, the kvs uses integer as the key type and maxintlattice as the value lattice.
typedef KV_Store<string, RC_KVS_PairLattice<string>> Database;

typedef consistent_hash_map<node_t,crc32_hasher> consistent_hash_t;

struct pair_hash {
    template <class T1, class T2>
    std::size_t operator () (const std::pair<T1,T2> &p) const {
        auto h1 = std::hash<T1>{}(p.first);
        auto h2 = std::hash<T2>{}(p.second);

        return h1 ^ h2;  
    }
};

struct coordination_data {
    unordered_map<string, RC_KVS_PairLattice<string>> data;
};

// global variable to keep track of the server addresses
tbb::concurrent_unordered_set<pair<string, size_t>, pair_hash>* server_addr;

// Handle request from clients
string process_client_request(unique_ptr<Database>& kvs, communication::Request& req, int& update_counter, unique_ptr<SetLattice<string>>& change_set, int& local_timestamp, int thread_id) {
    local_timestamp ++;
    communication::Response response;

    if (req.has_get()) {
        cout << "received get by thread " << thread_id << "\n";
        response.set_value(kvs->get(req.get().key()).reveal().value);
        response.set_succeed(true);
    }
    else if (req.has_put()) {
        cout << "received put by thread " << thread_id << "\n";
        timestamp_value_pair<string> p = timestamp_value_pair<string>(local_timestamp, req.put().value());
        kvs->put(req.put().key(), p);
        update_counter++;
        change_set->insert(req.put().key());
        response.set_succeed(true);
    }
    else {
        response.set_err(true);
        response.set_succeed(false);
    }
    string data;
    response.SerializeToString(&data);
    return data;
}

// Handle distributed gossip from threads on other nodes
void process_distributed_gossip(unique_ptr<Database>& kvs, communication::Gossip& gossip, int thread_id) {
    for (int i = 0; i < gossip.tuple_size(); i++) {
        timestamp_value_pair<string> p = timestamp_value_pair<string>(gossip.tuple(i).timestamp(), gossip.tuple(i).value());
        kvs->put(gossip.tuple(i).key(), p);
    }
}

// Handle local gossip from threads on the same node
void process_local_gossip(unique_ptr<Database>& kvs, coordination_data* c_data, int thread_id) {
    for (auto it = c_data->data.begin(); it != c_data->data.end(); it++) {
        kvs->put(it->first, it->second);
    }
    delete c_data;
}

void send_gossip(unique_ptr<Database>& kvs, unique_ptr<SetLattice<string>>& change_set, SocketCache& cache, consistent_hash_t& hash_ring, crc32_hasher& hasher, string ip, size_t port) {
    using address_t = string;
    address_t self_id = ip + ":" + to_string(port);
    unordered_map<address_t, coordination_data*> local_gossip_map;
    unordered_map<address_t, communication::Gossip> distributed_gossip_map;
    for (auto it = change_set->reveal().begin(); it != change_set->reveal().end(); it++) {
        vector<node_t> gossip_node;
        auto pos = hash_ring.find(hasher(*it));
        for (int i = 0; i < REPLICATION; i++) {
            if (pos->second.id_.compare(self_id) != 0) {
                cout << "dest id is " << pos->second.id_ << "\n";
                cout << "self id is " << self_id << "\n";
                gossip_node.push_back(pos->second);
            }
            if (++pos == hash_ring.end()) pos = hash_ring.begin();
        }
        for (auto node_it = gossip_node.begin(); node_it != gossip_node.end(); node_it++) {
            // gossip within the same node
            if (node_it->ip_.compare(ip) == 0) {
                // if the addr is not already in the map, we have to "new" the coordination data
                if (local_gossip_map.find(node_it->lgossip_addr_) == local_gossip_map.end()) {
                    local_gossip_map[node_it->lgossip_addr_] = new coordination_data;
                }
                local_gossip_map[node_it->lgossip_addr_]->data.emplace(*it, kvs->get(*it));
            }
            // gossip aross nodes
            else {
                communication::Gossip_Tuple* tp = distributed_gossip_map[node_it->dgossip_addr_].add_tuple();
                tp->set_key(*it);
                tp->set_value(kvs->get(*it).reveal().value);
                tp->set_timestamp(kvs->get(*it).reveal().timestamp);
            }
        }
    }
    // send local gossip
    for (auto it = local_gossip_map.begin(); it != local_gossip_map.end(); it++) {
        zmq_util::send_msg((void*)it->second, &cache[it->first]);
    }
    // send distributed gossip
    for (auto it = distributed_gossip_map.begin(); it != distributed_gossip_map.end(); it++) {
        string data;
        it->second.SerializeToString(&data);
        zmq_util::send_string(data, &cache[it->first]);
    }
}

bool responsible(string key, consistent_hash_t& hash_ring, crc32_hasher& hasher, string ip, size_t port) {
    using address_t = string;
    address_t self_id = ip + ":" + to_string(port);
    bool resp = false;
    auto pos = hash_ring.find(hasher(key));
    for (int i = 0; i < REPLICATION; i++) {
        if (pos->second.id_.compare(self_id) == 0) {
            resp = true;
        }
        if (++pos == hash_ring.end()) pos = hash_ring.begin();
    }
    return resp;
}

void redistribute(unique_ptr<Database>& kvs, SocketCache& cache, consistent_hash_t& hash_ring, crc32_hasher& hasher, string ip, size_t port, node_t dest_node) {
    // perform local gossip
    if (ip == dest_node.ip_) {
        cout << "local redistribute \n";
        coordination_data* c_data = new coordination_data;
        unordered_set<string> keys = kvs->keys();
        unordered_set<string> to_remove;
        for (auto it = keys.begin(); it != keys.end(); it++) {
            if (!responsible(*it, hash_ring, hasher, ip, port)) {
                to_remove.insert(*it);
            }
            if (responsible(*it, hash_ring, hasher, dest_node.ip_, dest_node.port_)) {
                cout << "node " + ip + " thread " + to_string(port) + " moving " + *it + " with value " + kvs->get(*it).reveal().value + " to node " + dest_node.ip_ + " thread " + to_string(dest_node.port_) + "\n";
                c_data->data.emplace(*it, kvs->get(*it));
            }            
        }
        for (auto it = to_remove.begin(); it != to_remove.end(); it++) {
            kvs->remove(*it);
        }
        zmq_util::send_msg((void*)c_data, &cache[dest_node.lgossip_addr_]);
    }
    // perform distributed gossip
    else {
        cout << "distributed redistribute \n";
        communication::Gossip gossip;
        unordered_set<string> keys = kvs->keys();
        unordered_set<string> to_remove;
        for (auto it = keys.begin(); it != keys.end(); it++) {
            if (!responsible(*it, hash_ring, hasher, ip, port)) {
                to_remove.insert(*it);
            }
            if (responsible(*it, hash_ring, hasher, dest_node.ip_, dest_node.port_)) {
                cout << "node " + ip + " thread " + to_string(port) + " moving " + *it + " with value " + kvs->get(*it).reveal().value + " to node " + dest_node.ip_ + " thread " + to_string(dest_node.port_) + "\n";
                communication::Gossip_Tuple* tp = gossip.add_tuple();
                tp->set_key(*it);
                tp->set_value(kvs->get(*it).reveal().value);
                tp->set_timestamp(kvs->get(*it).reveal().timestamp);
            }
        }
        for (auto it = to_remove.begin(); it != to_remove.end(); it++) {
            kvs->remove(*it);
        }        
        string data;
        gossip.SerializeToString(&data);
        zmq_util::send_string(data, &cache[dest_node.dgossip_addr_]);
    }
}

// Act as an event loop for the server
void *worker_routine (zmq::context_t* context, string ip, int thread_id, bool joining)
{
    size_t port = 6560 + thread_id;

    // initialize the thread's kvs replica
    unique_ptr<Database> kvs(new Database);
    // initialize a set lattice that keeps track of the keys that get updated
    unique_ptr<SetLattice<string>> change_set(new SetLattice<string>);
    // socket that listens for client requests
    zmq::socket_t responder(*context, ZMQ_REP);
    responder.bind("tcp://*:" + to_string(port - 100));
    // socket that listens for distributed gossip
    zmq::socket_t dgossip_puller(*context, ZMQ_PULL);
    dgossip_puller.bind("tcp://*:" + to_string(port));
    // socket that listens for local gossip
    zmq::socket_t lgossip_puller(*context, ZMQ_PULL);
    lgossip_puller.bind("inproc://" + to_string(port));
    // socket that listens for actor joining
    zmq::socket_t join_puller(*context, ZMQ_PULL);
    join_puller.bind("tcp://*:" + to_string(port + 100));
    // socket that listens for departure command
    zmq::socket_t depart_command_puller(*context, ZMQ_PULL);
    depart_command_puller.bind("inproc://" + to_string(port + 200));
    // socket that listens for actor departure
    zmq::socket_t depart_puller(*context, ZMQ_PULL);
    depart_puller.bind("tcp://*:" + to_string(port + 200));

    SocketCache cache(context, ZMQ_PUSH);

    // used to hash keys
    crc32_hasher hasher;
    // read in the current server addresses and build the hash ring
    consistent_hash_t hash_ring;
    for (auto it = server_addr->begin(); it != server_addr->end(); it++) {
        hash_ring.insert(node_t(it->first, it->second));
    }

    // this is a new thread joining
    if (joining) {
        // contact all other actors
        string addr = ip + ":" + to_string(port);
        for (auto it = hash_ring.begin(); it != hash_ring.end(); it++) {
            if (it->second.id_.compare(addr) != 0) {
                zmq_util::send_string(addr, &cache[(it->second).join_addr_]);
            }
        }
        // hard coded for now
        string client_addr = "tcp://35.164.92.185:" + to_string(6560 + 500);
        zmq_util::send_string("join:" + addr, &cache[client_addr]);
    }

    //  Initialize poll set
    vector<zmq::pollitem_t> pollitems = {
        { static_cast<void *>(responder), 0, ZMQ_POLLIN, 0 },
        { static_cast<void *>(dgossip_puller), 0, ZMQ_POLLIN, 0 },
        { static_cast<void *>(lgossip_puller), 0, ZMQ_POLLIN, 0 },
        { static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0 },
        { static_cast<void *>(depart_command_puller), 0, ZMQ_POLLIN, 0 },
        { static_cast<void *>(depart_puller), 0, ZMQ_POLLIN, 0 }
    };

    // LWW timestamp
    int local_timestamp = 0;
    // A counter that keep track of the number of updates performed to the kvs before the last gossip
    int update_counter = 0;

    // Enter the event loop
    while (true) {
        zmq_util::poll(-1, &pollitems);

        // If there is a request from clients
        if (pollitems[0].revents & ZMQ_POLLIN) {
            string data = zmq_util::recv_string(&responder);
            communication::Request req;
            req.ParseFromString(data);
            //  Process request
            string result = process_client_request(kvs, req, update_counter, change_set, local_timestamp, thread_id);
            //  Send reply back to client
            zmq_util::send_string(result, &responder);
        }

        // If there is gossip from threads on other nodes
        if (pollitems[1].revents & ZMQ_POLLIN) {
            cout << "received distributed gossip\n";
            string data = zmq_util::recv_string(&dgossip_puller);
            communication::Gossip gossip;
            gossip.ParseFromString(data);
            //  Process distributed gossip
            process_distributed_gossip(kvs, gossip, thread_id);
        }

        // If there is gossip from threads on the same node
        if (pollitems[2].revents & ZMQ_POLLIN) {
            cout << "received local gossip\n";
            //  Process local gossip
            zmq::message_t msg;
            zmq_util::recv_msg(&lgossip_puller, msg);
            coordination_data* c_data = *(coordination_data **)(msg.data());
            process_local_gossip(kvs, c_data, thread_id);
        }

        // If there is actor joining
        if (pollitems[3].revents & ZMQ_POLLIN) {
            cout << "received joining\n";
            vector<string> v;
            split(zmq_util::recv_string(&join_puller), ':', v);
            // update hash ring and server addr
            hash_ring.insert(node_t(v[0], stoi(v[1])));
            if (thread_id == 1) server_addr->insert(make_pair(v[0], stoi(v[1])));
            redistribute(kvs, cache, hash_ring, hasher, ip, port, node_t(v[0], stoi(v[1])));
        }

        // If receives a departure command
        if (pollitems[4].revents & ZMQ_POLLIN) {
            cout << "THREAD " + to_string(thread_id) + " received departure command\n";
            // update hash ring
            hash_ring.erase(node_t(ip, port));
            string addr = ip + ":" + to_string(port);
            for (auto it = hash_ring.begin(); it != hash_ring.end(); it++) {
                zmq_util::send_string(addr, &cache[(it->second).depart_addr_]);
            }
            // hard coded for now
            string client_addr = "tcp://35.164.92.185:" + to_string(6560 + 500);
            zmq_util::send_string("depart:" + addr, &cache[client_addr]);
            if (hash_ring.size() != 0) {
                unique_ptr<SetLattice<string>> key_set(new SetLattice<string>(kvs->keys()));
                send_gossip(kvs, key_set, cache, hash_ring, hasher, ip, port);
            }
            break;
        }

        // If there is actor departing
        if (pollitems[5].revents & ZMQ_POLLIN) {
            cout << "received departing\n";
            vector<string> v;
            split(zmq_util::recv_string(&depart_puller), ':', v);
            // update hash ring and server addr
            hash_ring.erase(node_t(v[0], stoi(v[1])));
            if (thread_id == 1) server_addr->unsafe_erase(make_pair(v[0], stoi(v[1])));
        }

        if (update_counter >= THRESHOLD) {
            cout << "sending gossip\n";
            send_gossip(kvs, change_set, cache, hash_ring, hasher, ip, port);
            // Reset the change_set and update_counter
            change_set.reset(new SetLattice<string>);
            update_counter = 0;
            //cout << "The gossip is sent by thread " << thread_id << "\n";
        }
    }
    return (NULL);
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        cerr << "usage:" << argv[0] << " <server_address> <new_node>" << endl;
        return 1;
    }
    if (string(argv[2]) != "y" && string(argv[2]) != "n") {
        cerr << "invalid argument" << endl;
        return 1;
    }
    string ip = argv[1];
    string new_node = argv[2];
    //  Prepare our context
    zmq::context_t context(1);

    SocketCache cache(&context, ZMQ_PUSH);

    set<int> active_thread_id = set<int>();
    for (int i = 1; i <= THREAD_NUM; i++) {
        active_thread_id.insert(i);
    }

    server_addr = new tbb::concurrent_unordered_set<pair<string, size_t>, pair_hash>();

    // read server address from the file
    if (new_node == "n") {
        string ip_line;
        ifstream address;
        address.open("/home/ubuntu/research/tiered-storage/kv_store/lww_kvs/server_address.txt");
        while (getline(address, ip_line)) {
            for (int i = 1; i <= THREAD_NUM; i++) {
                server_addr->insert(make_pair(ip_line, (6560 + i)));
            }
        }
        address.close();
    }
    // get server address from the seed node
    else {
        string ip_line;
        ifstream address;
        address.open("/home/ubuntu/research/tiered-storage/kv_store/lww_kvs/seed_address.txt");
        getline(address, ip_line);       
        address.close();
        cout << "before zmq\n";
        cout << ip_line + "\n";
        zmq::socket_t addr_requester(context, ZMQ_REQ);
        addr_requester.connect("tcp://" + ip_line + ":" + to_string(6560));
        cout << "before sending req\n";
        zmq_util::send_string("join", &addr_requester);
        vector<string> addresses;
        cout << "after sending req\n";
        split(zmq_util::recv_string(&addr_requester), '|', addresses);
        for (auto it = addresses.begin(); it != addresses.end(); it++) {
            vector<string> address;
            split(*it, ':', address);
            server_addr->insert(make_pair(address[0], stoi(address[1])));
        }
    }

    for (auto it = server_addr->begin(); it != server_addr->end(); it++) {
        cout << "address is " + it->first + ":" + to_string(it->second) + "\n";
    }

    vector<thread> threads;

    if (new_node == "n") {
        for (int thread_id = 1; thread_id <= THREAD_NUM; thread_id++) {
            threads.push_back(thread(worker_routine, &context, ip, thread_id, false));
        }        
    }
    else {
        for (int thread_id = 1; thread_id <= THREAD_NUM; thread_id++) {
            server_addr->insert(make_pair(ip, 6560 + thread_id));
            threads.push_back(thread(worker_routine, &context, ip, thread_id, true));
        }          
    }

    zmq::socket_t addr_responder(context, ZMQ_REP);
    addr_responder.bind("tcp://*:" + to_string(6560));

    zmq_pollitem_t pollitems [2];
    pollitems[0].socket = static_cast<void *>(addr_responder);
    pollitems[0].events = ZMQ_POLLIN;
    pollitems[1].socket = NULL;
    pollitems[1].fd = 0;
    pollitems[1].events = ZMQ_POLLIN;

    string input;
    int next_thread_id = THREAD_NUM + 1;
    while (true) {
        zmq::poll(pollitems, 2, -1);
        if (pollitems[0].revents & ZMQ_POLLIN) {
            string request = zmq_util::recv_string(&addr_responder);
            cout << "request is " + request + "\n";
            if (request == "join") {
                string addresses;
                for (auto it = server_addr->begin(); it != server_addr->end(); it++) {
                    addresses += (it->first + ":" + to_string(it->second) + "|");
                }
                addresses.pop_back();
                zmq_util::send_string(addresses, &addr_responder);
            }
            else {
                cout << "invalid request\n";
            }
        }
        else {
            getline(cin, input);
            if (input == "ADD") {
                cout << "adding thread\n";
                threads.push_back(thread(worker_routine, &context, ip, next_thread_id, true));
                active_thread_id.insert(next_thread_id);
                server_addr->insert(make_pair(ip, 6560 + next_thread_id));
                next_thread_id += 1;
            }
            else if (input == "REMOVE") {
                cout << "removing thread\n";
                zmq_util::send_string("depart", &cache["inproc://" + to_string(6560 + *(active_thread_id.rbegin()) + 200)]);
                active_thread_id.erase(*(active_thread_id.rbegin()));
            }
            else {
                cout << "Invalid Request\n";
            }
            cout << "Active thread ids are:\n";
            for (auto it = active_thread_id.begin(); it != active_thread_id.end(); it++) {
                cout << *it << " ";
            }
            cout << "\n";
        }
    }
    for (auto& th: threads) th.join();
    return 0;
}
