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

struct coordination_data {
    unordered_map<string, RC_KVS_PairLattice<string>> data;
};

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

void send_gossip(unique_ptr<Database> &kvs, unique_ptr<SetLattice<string>>& change_set, SocketCache& cache, consistent_hash_t& hash_ring, crc32_hasher& hasher, string ip, size_t port, int thread_id) {
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

    // for (int i = 0; i != THREAD_NUM; i++) {
    //     if (i != thread_id) {
    //         string addr = "inproc://" + to_string(6560 + thread_id);
    //         coordination_data* c_data = new coordination_data;
    //         for (auto it = change_set->reveal().begin(); it != change_set->reveal().end(); it++) {
    //             c_data->data.emplace(*it, kvs->get(*it));
    //         }
    //         zmq_util::send_msg((void*)c_data, &cache[addr]);
    //     }
    // }
}

//TODO see how push/pop socket works
// void send_distributed_gossip(SetLattice<string> &change_set, unique_ptr<Database> &kvs, zmq::socket_t &push_d){
//     communication::Request request;

//     for (auto it = change_set.reveal().begin(); it != change_set.reveal().end(); it++) {
//         communication::Request_Gossip_Tuple* tp = request.mutable_gossip()->add_tuple();
//         tp -> set_key(*it);
//         tp -> set_value(kvs->get(*it).reveal().value);
//         tp -> set_timestamp(kvs->get(*it).reveal().timestamp);
//     }

//     string data;
//     request.SerializeToString(&data);

//     zmq_msg_t msg;
//     zmq_msg_init_size(&msg, data.size());
//     memcpy(zmq_msg_data(&msg), &(data[0]), data.size());
//     zmq_msg_send(&msg, static_cast<void *>(push_d), 0);
// }

// TODO: argument: key set
// void send_local_gossip(SetLattice<string> &change_set, unique_ptr<Database> &kvs, zmq::socket_t &push_l){
//     coordination_data *c_data = new coordination_data;
//     for (auto it = change_set.reveal().begin(); it != change_set.reveal().end(); it++) {
//         c_data->data.emplace(*it, kvs->get(*it));
//     }

//     zmq_msg_t msg;
//     zmq_msg_init_size(&msg, sizeof(coordination_data**));
//     memcpy(zmq_msg_data(&msg), &c_data, sizeof(coordination_data**));
//     zmq_msg_send(&msg, static_cast<void *>(push_l), 0);
// }

// Act as an event loop for the server
void *worker_routine (zmq::context_t* context, string ip, int thread_id)
{
    size_t port = 6560 + thread_id;
    int local_timestamp = 0;
    // initialize the thread's kvs replica
    unique_ptr<Database> kvs(new Database);
    // initialize a set lattice that keeps track of the keys that get updated
    unique_ptr<SetLattice<string>> change_set(new SetLattice<string>);
    // read in the initial server addresses and build the hash ring
    consistent_hash_t hash_ring;
    string ip_line;
    ifstream address;
    address.open("/home/ubuntu/research/high-performance-lattices/kv_store/lww_kvs/server_address.txt");
    while (getline(address, ip_line)) {
        for (int i = 0; i < THREAD_NUM; i++) {
            hash_ring.insert(node_t(ip_line, 6560 + i));
        }
    }
    address.close();
    // used to hash keys
    crc32_hasher hasher;
    // socket that listens for client requests
    zmq::socket_t responder(*context, ZMQ_REP);
    responder.bind("tcp://*:" + to_string(5560 + thread_id));
    // socket that listens for distributed gossip
    zmq::socket_t dgossip_puller(*context, ZMQ_PULL);
    dgossip_puller.bind("tcp://*:" + to_string(6560 + thread_id));
    // socket that listens for local gossip
    zmq::socket_t lgossip_puller(*context, ZMQ_PULL);
    lgossip_puller.bind("inproc://" + to_string(6560 + thread_id));

    SocketCache cache(context, ZMQ_PUSH);

    //  Initialize poll set
    vector<zmq::pollitem_t> pollitems = {
        { static_cast<void *>(responder), 0, ZMQ_POLLIN, 0 },
        { static_cast<void *>(dgossip_puller), 0, ZMQ_POLLIN, 0 },
        { static_cast<void *>(lgossip_puller), 0, ZMQ_POLLIN, 0 }
    };

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

        if (update_counter >= THRESHOLD && THREAD_NUM != 1) {
            cout << "sending gossip\n";
            send_gossip(kvs, change_set, cache, hash_ring, hasher, ip, port, thread_id);
            // Reset the change_set and update_counter
            change_set.reset(new SetLattice<string>);
            update_counter = 0;
            //cout << "The gossip is sent by thread " << thread_id << "\n";
        }
    }
    return (NULL);
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        cerr << "usage:" << argv[0] << " <server_address>" << endl;
        return 1;
    }
    string ip = argv[1];
    //  Prepare our context
    zmq::context_t context(1);

    //  Launch pool of worker threads
    //cout << "Starting the server with " << THREAD_NUM << " threads and gossip threshold " << THRESHOLD << "\n";

    vector<thread> threads;
    for (int thread_id = 0; thread_id != THREAD_NUM; thread_id++) {
        threads.push_back(thread(worker_routine, &context, ip, thread_id));
    }
    for (auto& th: threads) th.join();
    return 0;
}
