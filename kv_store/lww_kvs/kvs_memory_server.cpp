#include <zmq.hpp>
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <cstdio>
#include <pthread.h>
#include <unistd.h>
#include <memory>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <ctime>
#include "rc_kv_store.h"
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"
#include "consistent_hash_map.hpp"
#include "common.h"
#include "server_utility.h"

//#define GetCurrentDir getcwd

using namespace std;

// If the total number of updates to the kvs before the last gossip reaches THRESHOLD, then the thread gossips to others.
#define THRESHOLD 1

// Define the gossip period (frequency)
#define PERIOD 5

// Define the number of memory threads
#define MEMORY_THREAD_NUM 3

// For simplicity, the kvs uses integer as the key type and maxintlattice as the value lattice.
typedef Concurrent_KV_Store<string, RC_KVS_PairLattice<string>> Database;

/*std::string GetCurrentWorkingDir( void ) {
  char buff[FILENAME_MAX];
  GetCurrentDir( buff, FILENAME_MAX );
  std::string current_working_dir(buff);
  return current_working_dir;
}*/

struct pair_hash {
    template <class T1, class T2>
    std::size_t operator () (const std::pair<T1,T2> &p) const {
        auto h1 = std::hash<T1>{}(p.first);
        auto h2 = std::hash<T2>{}(p.second);

        return h1 ^ h2;  
    }
};

typedef unordered_map<string, RC_KVS_PairLattice<string>> gossip_data;

typedef pair<size_t, unordered_set<string>> changeset_data;

typedef unordered_map<string, unordered_set<string>> changeset_address;

typedef unordered_map<string, unordered_set<pair<string, bool>, pair_hash>> redistribution_address;

struct key_info {
    key_info(): tier_('M'), global_memory_replication_(GLOBAL_MEMORY_REPLICATION) {}
    key_info(char tier, int global_memory_replication): tier_(tier), global_memory_replication_(global_memory_replication) {}
    char tier_;
    int global_memory_replication_;
};

atomic<int> lww_timestamp(0);

pair<RC_KVS_PairLattice<string>, bool> process_get(string key, Database* kvs) {
    return pair<RC_KVS_PairLattice<string>, bool>(kvs->get(key), true);
}

bool process_put(string key, int timestamp, string value, int thread_id, Database* kvs) {
    timestamp_value_pair<string> p = timestamp_value_pair<string>(timestamp, value);
    kvs->put(key, RC_KVS_PairLattice<string>(p));
    return true;
}

// Handle request from clients
string process_client_request(communication::Request& req, int thread_id, unordered_set<string>& local_changeset, Database* kvs) {
    communication::Response response;

    if (req.has_get()) {
        cout << "received get by thread " << thread_id << "\n";
        auto res = process_get(req.get().key(), kvs);
        response.set_succeed(res.second);
        response.set_value(res.first.reveal().value);
    }
    else if (req.has_put()) {
        cout << "received put by thread " << thread_id << "\n";
        response.set_succeed(process_put(req.put().key(), lww_timestamp.load(), req.put().value(), thread_id, kvs));
        local_changeset.insert(req.put().key());
    }
    else {
        response.set_succeed(false);
    }
    string data;
    response.SerializeToString(&data);
    return data;
}

// Handle distributed gossip from threads on other nodes
void process_distributed_gossip(communication::Gossip& gossip, int thread_id, Database* kvs) {
    for (int i = 0; i < gossip.tuple_size(); i++) {
        process_put(gossip.tuple(i).key(), gossip.tuple(i).timestamp(), gossip.tuple(i).value(), thread_id, kvs);
    }
}

void send_gossip(changeset_address* change_set_addr, SocketCache& cache, string ip, int thread_id, Database* kvs) {
    unordered_map<string, communication::Gossip> distributed_gossip_map;
    for (auto map_it = change_set_addr->begin(); map_it != change_set_addr->end(); map_it++) {
        vector<string> v;
        split(map_it->first, ':', v);
        // add to distribute gossip map
        for (auto set_it = map_it->second.begin(); set_it != map_it->second.end(); set_it++) {
            cout << "distribute gossip key: " + *set_it + " by thread " + to_string(thread_id) + "\n";
            communication::Gossip_Tuple* tp = distributed_gossip_map[worker_node_t(v[0], stoi(v[1])).distributed_gossip_connect_addr_].add_tuple();
            tp->set_key(*set_it);
            auto res = process_get(*set_it, kvs);
            tp->set_value(res.first.reveal().value);
            tp->set_timestamp(res.first.reveal().timestamp);
        }
    }
    // send distributed gossip
    for (auto it = distributed_gossip_map.begin(); it != distributed_gossip_map.end(); it++) {
        string data;
        it->second.SerializeToString(&data);
        zmq_util::send_string(data, &cache[it->first]);
    }
}

/*template<typename N, typename H>
bool responsible(string key, int rep, consistent_hash_map<N,H>& hash_ring, string ip, size_t port, node_t& sender_node, bool& remove) {
    string self_id = ip + ":" + to_string(port);
    bool resp = false;
    auto pos = hash_ring.find(key);
    auto target_pos = hash_ring.begin();
    for (int i = 0; i < rep; i++) {
        if (pos->second.id_.compare(self_id) == 0) {
            resp = true;
            target_pos = pos;
        }
        if (++pos == hash_ring.end()) pos = hash_ring.begin();
    }
    if (resp && (hash_ring.size() > rep)) {
        remove = true;
        sender_node = pos->second;
    }
    else if (resp && (hash_ring.size() <= rep)) {
        remove = false;
        if (++target_pos == hash_ring.end()) target_pos = hash_ring.begin();
        sender_node = target_pos->second;
    }
    return resp;
}*/

// memory worker event loop
void memory_worker_routine (zmq::context_t* context, Database* kvs, string ip, int thread_id)
{
    size_t port = SERVER_PORT + thread_id;

    unordered_set<string> local_changeset;

    // socket that respond to client requests
    zmq::socket_t responder(*context, ZMQ_REP);
    responder.bind(worker_node_t(ip, port).client_connection_bind_addr_);
    // socket that listens for distributed gossip
    zmq::socket_t dgossip_puller(*context, ZMQ_PULL);
    dgossip_puller.bind(worker_node_t(ip, port).distributed_gossip_bind_addr_);
    // socket that listens for redistribution
    zmq::socket_t lredistribute_puller(*context, ZMQ_PULL);
    lredistribute_puller.bind(worker_node_t(ip, port).local_redistribute_addr_);

    // used to communicate with master thread for changeset addresses
    zmq::socket_t changeset_address_requester(*context, ZMQ_REQ);
    changeset_address_requester.connect(master_node_t(ip).changeset_addr_);

    // used to send gossip
    SocketCache cache(context, ZMQ_PUSH);

    //  Initialize poll set
    vector<zmq::pollitem_t> pollitems = {
        { static_cast<void *>(responder), 0, ZMQ_POLLIN, 0 },
        { static_cast<void *>(dgossip_puller), 0, ZMQ_POLLIN, 0 },
        { static_cast<void *>(lredistribute_puller), 0, ZMQ_POLLIN, 0 },
    };

    auto start = std::chrono::system_clock::now();
    auto end = std::chrono::system_clock::now();

    // Enter the event loop
    while (true) {
        zmq_util::poll(0, &pollitems);

        // If there is a request from clients
        if (pollitems[0].revents & ZMQ_POLLIN) {
            string data = zmq_util::recv_string(&responder);
            communication::Request req;
            req.ParseFromString(data);
            //  Process request
            string result = process_client_request(req, thread_id, local_changeset, kvs);
            //  Send reply back to client
            zmq_util::send_string(result, &responder);
        }

        // If there is gossip from threads on other nodes
        if (pollitems[1].revents & ZMQ_POLLIN) {
            cout << "received distributed gossip by thread " + to_string(thread_id) + "\n";
            string data = zmq_util::recv_string(&dgossip_puller);
            communication::Gossip gossip;
            gossip.ParseFromString(data);
            //  Process distributed gossip
            process_distributed_gossip(gossip, thread_id, kvs);
        }

        // If receives a local redistribute command
        if (pollitems[2].revents & ZMQ_POLLIN) {
            cout << "received local redistribute request by thread " + to_string(thread_id) + "\n";
            /*zmq::message_t msg;
            zmq_util::recv_msg(&lredistribute_puller, msg);
            redistribution_address* r_data = *(redistribution_address **)(msg.data());
            changeset_address c_address;
            unordered_set<string> remove_set;
            for (auto map_it = r_data->begin(); map_it != r_data->end(); map_it++) {
                for (auto set_it = map_it->second.begin(); set_it != map_it->second.end(); set_it++) {
                    c_address[map_it->first].insert(set_it->first);
                    if (set_it->second)
                        remove_set.insert(set_it->first);
                }
            }
            send_gossip(&c_address, cache, ip, thread_id);
            delete r_data;
            // remove keys in the remove set
            for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
                key_set.erase(*it);
                string fname = "/home/ubuntu/ebs/ebs_" + to_string(thread_id) + "/" + *it;
                if( remove( fname.c_str() ) != 0 )
                    perror( "Error deleting file" );
                else
                    puts( "File successfully deleted" );
            }*/
        }

        end = std::chrono::system_clock::now();
        if (chrono::duration_cast<std::chrono::seconds>(end-start).count() >= PERIOD || local_changeset.size() >= THRESHOLD) {
            if (local_changeset.size() >= THRESHOLD)
                cout << "reached gossip threshold\n";
            if (local_changeset.size() > 0) {
                changeset_data* data = new changeset_data();
                data->first = port;
                for (auto it = local_changeset.begin(); it != local_changeset.end(); it++) {
                    (data->second).insert(*it);
                }
                zmq_util::send_msg((void*)data, &changeset_address_requester);
                zmq::message_t msg;
                zmq_util::recv_msg(&changeset_address_requester, msg);
                changeset_address* res = *(changeset_address **)(msg.data());
                send_gossip(res, cache, ip, thread_id, kvs);
                delete res;
                local_changeset.clear();
            }
            start = std::chrono::system_clock::now();
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        cerr << "usage:" << argv[0] << " <new_node>" << endl;
        return 1;
    }
    if (string(argv[1]) != "y" && string(argv[1]) != "n") {
        cerr << "invalid argument" << endl;
        return 1;
    }

    //std::cout << GetCurrentWorkingDir() << std::endl;
    Database* kvs = new Database();

    string ip = getIP();
    string new_node = argv[1];
    //  Prepare our context
    zmq::context_t context(1);

    SocketCache cache(&context, ZMQ_PUSH);

    SocketCache key_address_requesters(&context, ZMQ_REQ);

    global_hash_t global_hash_ring;

    unordered_map<string, key_info> placement;

    //unordered_map<string, unordered_map<string, unordered_set<string>>> change_map;

    unordered_set<string> client_address;

    // read client address from the file
    string ip_line;
    ifstream address;
    address.open("build/kv_store/lww_kvs/client_address.txt");
    while (getline(address, ip_line)) {
        client_address.insert(ip_line);
    }
    address.close();

    // read server address from the file
    if (new_node == "n") {
        address.open("build/kv_store/lww_kvs/server_address.txt");
        while (getline(address, ip_line)) {
            global_hash_ring.insert(master_node_t(ip_line));
        }
        address.close();
    }
    // get server address from the seed node
    else {
        address.open("build/kv_store/lww_kvs/seed_address.txt");
        getline(address, ip_line);       
        address.close();
        //cout << "before zmq\n";
        //cout << ip_line + "\n";
        zmq::socket_t addr_requester(context, ZMQ_REQ);
        addr_requester.connect(master_node_t(ip_line).seed_connection_connect_addr_);
        //cout << "before sending req\n";
        zmq_util::send_string("join", &addr_requester);
        vector<string> addresses;
        //cout << "after sending req\n";
        split(zmq_util::recv_string(&addr_requester), '|', addresses);
        for (auto it = addresses.begin(); it != addresses.end(); it++) {
            global_hash_ring.insert(master_node_t(*it));
        }
        // add itself to global hash ring
        global_hash_ring.insert(master_node_t(ip));
    }

    for (auto it = global_hash_ring.begin(); it != global_hash_ring.end(); it++) {
        cout << "address is " + it->second.ip_ + "\n";
    }


    vector<thread> memory_threads;

    for (int thread_id = 1; thread_id <= MEMORY_THREAD_NUM; thread_id++) {
        memory_threads.push_back(thread(memory_worker_routine, &context, kvs, ip, thread_id));
    }

    if (new_node == "y") {
        // notify other servers
        for (auto it = global_hash_ring.begin(); it != global_hash_ring.end(); it++) {
            if (it->second.ip_.compare(ip) != 0) {
                zmq_util::send_string(ip, &cache[(it->second).node_join_connect_addr_]);
            }
        }
    }

    // notify clients
    for (auto it = client_address.begin(); it != client_address.end(); it++) {
      zmq_util::send_string("join:" + ip, &cache[master_node_t(*it).client_notify_connect_addr_]);
    }

    // (seed node) responsible for sending the server address to the new node
    zmq::socket_t addr_responder(context, ZMQ_REP);
    addr_responder.bind(master_node_t(ip).seed_connection_bind_addr_);
    // listens for node joining
    zmq::socket_t join_puller(context, ZMQ_PULL);
    join_puller.bind(master_node_t(ip).node_join_bind_addr_);
    // listens for node departing
    zmq::socket_t depart_puller(context, ZMQ_PULL);
    depart_puller.bind(master_node_t(ip).node_depart_bind_addr_);
    // responsible for sending the worker address (responsible for the requested key) to the client or other servers
    zmq::socket_t key_address_responder(context, ZMQ_REP);
    key_address_responder.bind(master_node_t(ip).key_exchange_bind_addr_);
    // responsible for responding changeset addresses from workers
    zmq::socket_t changeset_address_responder(context, ZMQ_REP);
    changeset_address_responder.bind(master_node_t(ip).changeset_addr_);

    zmq_pollitem_t pollitems [6];
    pollitems[0].socket = static_cast<void *>(addr_responder);
    pollitems[0].events = ZMQ_POLLIN;
    pollitems[1].socket = static_cast<void *>(join_puller);
    pollitems[1].events = ZMQ_POLLIN;
    pollitems[2].socket = static_cast<void *>(depart_puller);
    pollitems[2].events = ZMQ_POLLIN;
    pollitems[3].socket = static_cast<void *>(key_address_responder);
    pollitems[3].events = ZMQ_POLLIN;
    pollitems[4].socket = static_cast<void *>(changeset_address_responder);
    pollitems[4].events = ZMQ_POLLIN;
    pollitems[5].socket = NULL;
    pollitems[5].fd = 0;
    pollitems[5].events = ZMQ_POLLIN;

    string input;
    while (true) {
        zmq::poll(pollitems, 6, -1);
        if (pollitems[0].revents & ZMQ_POLLIN) {
            string request = zmq_util::recv_string(&addr_responder);
            cout << "request is " + request + "\n";
            if (request == "join") {
                string addresses;
                for (auto it = global_hash_ring.begin(); it != global_hash_ring.end(); it++) {
                    addresses += (it->second.ip_ + "|");
                }
                addresses.pop_back();
                zmq_util::send_string(addresses, &addr_responder);
            }
            else {
                cout << "invalid request\n";
            }
        }
        else if (pollitems[1].revents & ZMQ_POLLIN) {
            cout << "received joining\n";
            /*string new_server_ip = zmq_util::recv_string(&join_puller);
            // update hash ring
            global_hash_ring.insert(master_node_t(new_server_ip));
            // instruct its workers to send gossip to the new server! (2 phase)
            unordered_map<string, redistribution_address*> redistribution_map;
            unordered_set<string> key_to_query;
            unordered_map<string, bool> key_remove_map;
            for (auto it = placement.begin(); it != placement.end(); it++) {
                master_node_t sender_node;
                bool remove = false;
                // use global replication factor for all keys for now
                bool resp = responsible<master_node_t, crc32_hasher>(it->first, GLOBAL_EBS_REPLICATION, global_hash_ring, new_server_ip, SERVER_PORT, sender_node, remove);
                if (resp && (sender_node.ip_.compare(ip) == 0)) {
                    key_to_query.insert(it->first);
                    key_remove_map[it->first] = remove;
                }
            }
            // send key address request
            communication::Key_Request req;
            req.set_sender("server");
            for (auto it = key_to_query.begin(); it != key_to_query.end(); it++) {
                communication::Key_Request_Tuple* tp = req.add_tuple();
                tp->set_key(*it);
            }
            string key_req;
            req.SerializeToString(&key_req);
            zmq_util::send_string(key_req, &key_address_requesters[master_node_t(new_server_ip).key_exchange_connect_addr_]);
            string key_res = zmq_util::recv_string(&key_address_requesters[master_node_t(new_server_ip).key_exchange_connect_addr_]);
            communication::Key_Response resp;
            resp.ParseFromString(key_res);
            for (int i = 0; i < resp.tuple_size(); i++) {
                for (int j = 0; j < resp.tuple(i).address_size(); j++) {
                    string key = resp.tuple(i).key();
                    string target_address = resp.tuple(i).address(j).addr();
                    auto pos = ebs_hash_ring.find(key);
                    for (int k = 0; k < placement[key].local_ebs_replication_; k++) {
                        string worker_address = pos->second.local_redistribute_addr_;
                        if (redistribution_map.find(worker_address) == redistribution_map.end())
                            redistribution_map[worker_address] = new redistribution_address();
                        (*redistribution_map[worker_address])[target_address].insert(pair<string, bool>(key, key_remove_map[key]));
                        if (++pos == ebs_hash_ring.end()) pos = ebs_hash_ring.begin();
                    }

                }
            }
            for (auto it = redistribution_map.begin(); it != redistribution_map.end(); it++) {
                zmq_util::send_msg((void*)it->second, &cache[it->first]);
            }*/
        }
        else if (pollitems[2].revents & ZMQ_POLLIN) {
            cout << "received departure of other nodes\n";
            /*string departing_server_ip = zmq_util::recv_string(&depart_puller);
            // update hash ring
            global_hash_ring.erase(master_node_t(departing_server_ip));*/
        }
        else if (pollitems[3].revents & ZMQ_POLLIN) {
            cout << "received key address request\n";
            lww_timestamp++;
            string key_req = zmq_util::recv_string(&key_address_responder);
            communication::Key_Request req;
            req.ParseFromString(key_req);
            string sender = req.sender();
            communication::Key_Response res;
            // received worker thread address request (for a given key) from the client proxy
            if (sender == "client") {
                string key = req.tuple(0).key();
                cout << "key requested is " + key + "\n";
                // update placement metadata
                if (placement.find(key) == placement.end())
                    placement[key] = key_info('M', GLOBAL_MEMORY_REPLICATION);
                // for now, randomly select a memory thread
                size_t random_port = SERVER_PORT + rand()%MEMORY_THREAD_NUM + 1;
                string worker_address = worker_node_t(ip, random_port).client_connection_connect_addr_;
                cout << "worker address is " + worker_address + "\n";
                communication::Key_Response_Tuple* tp = res.add_tuple();
                tp->set_key(key);
                communication::Key_Response_Address* tp_addr = tp->add_address();
                tp_addr->set_addr(worker_address);
                string response;
                res.SerializeToString(&response);
                zmq_util::send_string(response, &key_address_responder);
            }
            // received worker thread address request (for a set of keys) from another server node
            else if (sender == "server") {
                size_t random_port = SERVER_PORT + rand()%MEMORY_THREAD_NUM + 1;
                string worker_address = worker_node_t(ip, random_port).id_;
                for (int i = 0; i < req.tuple_size(); i++) {
                    communication::Key_Response_Tuple* tp = res.add_tuple();
                    string key = req.tuple(i).key();
                    tp->set_key(key);
                    cout << "key requested is " + key + "\n";
                    // update placement metadata
                    if (placement.find(key) == placement.end())
                        placement[key] = key_info('M', GLOBAL_MEMORY_REPLICATION);
                    communication::Key_Response_Address* tp_addr = tp->add_address();
                    tp_addr->set_addr(worker_address);
                }
                string response;
                res.SerializeToString(&response);
                zmq_util::send_string(response, &key_address_responder);
            }
            else {
                cout << "Invalid sender \n";
            }
        }
        else if (pollitems[4].revents & ZMQ_POLLIN) {
            cout << "received changeset address request from the worker threads (for gossiping)\n";
            zmq::message_t msg;
            zmq_util::recv_msg(&changeset_address_responder, msg);
            changeset_data* data = *(changeset_data **)(msg.data());
            changeset_address* res = new changeset_address();
            unordered_map<master_node_t, unordered_set<string>, node_hash> node_map;
            for (auto it = data->second.begin(); it != data->second.end(); it++) {
                string key = *it;
                // check the global ring and request worker addresses from other node's master thread
                auto pos = global_hash_ring.find(key);
                for (int i = 0; i < placement[key].global_memory_replication_; i++) {
                    if (pos->second.ip_.compare(ip) != 0) {
                        node_map[pos->second].insert(key);
                    }
                    if (++pos == global_hash_ring.end()) pos = global_hash_ring.begin();
                }
            }
            for (auto map_iter = node_map.begin(); map_iter != node_map.end(); map_iter++) {
                // send key address request
                communication::Key_Request req;
                req.set_sender("server");
                for (auto set_iter = map_iter->second.begin(); set_iter != map_iter->second.end(); set_iter++) {
                    communication::Key_Request_Tuple* tp = req.add_tuple();
                    tp->set_key(*set_iter);
                }
                string key_req;
                req.SerializeToString(&key_req);
                zmq_util::send_string(key_req, &key_address_requesters[map_iter->first.key_exchange_connect_addr_]);
                string key_res = zmq_util::recv_string(&key_address_requesters[map_iter->first.key_exchange_connect_addr_]);
                communication::Key_Response resp;
                resp.ParseFromString(key_res);
                for (int i = 0; i < resp.tuple_size(); i++) {
                    for (int j = 0; j < resp.tuple(i).address_size(); j++) {
                        (*res)[resp.tuple(i).address(j).addr()].insert(resp.tuple(i).key());
                    }
                }
            }
            zmq_util::send_msg((void*)res, &changeset_address_responder);
            delete data;
        }
        else {
            /*getline(cin, input);
            if (input == "DEPART") {
                cout << "node departing\n";
                global_hash_ring.erase(master_node_t(ip));
                for (auto it = global_hash_ring.begin(); it != global_hash_ring.end(); it++) {
                    zmq_util::send_string(ip, &cache[it->second.node_depart_connect_addr_]);
                }
                // form the key_request map
                unordered_map<string, communication::Key_Request> key_request_map;
                for (auto it = placement.begin(); it != placement.end(); it++) {
                    string key = it->first;
                    auto pos = global_hash_ring.find(key);
                    for (int i = 0; i < placement[key].global_ebs_replication_; i++) {
                        key_request_map[pos->second.key_exchange_connect_addr_].set_sender("server");
                        communication::Key_Request_Tuple* tp = key_request_map[pos->second.key_exchange_connect_addr_].add_tuple();
                        tp->set_key(key);
                        if (++pos == global_hash_ring.end()) pos = global_hash_ring.begin();
                    }
                }
                unordered_map<string, redistribution_address*> redistribution_map;
                // send key addrss requests to other server nodes
                for (auto it = key_request_map.begin(); it != key_request_map.end(); it++) {
                    string key_req;
                    it->second.SerializeToString(&key_req);
                    zmq_util::send_string(key_req, &key_address_requesters[it->first]);
                    string key_res = zmq_util::recv_string(&key_address_requesters[it->first]);
                    communication::Key_Response resp;
                    resp.ParseFromString(key_res);
                    for (int i = 0; i < resp.tuple_size(); i++) {
                        for (int j = 0; j < resp.tuple(i).address_size(); j++) {
                            string key = resp.tuple(i).key();
                            string target_address = resp.tuple(i).address(j).addr();
                            auto pos = ebs_hash_ring.find(key);
                            for (int k = 0; k < placement[key].local_ebs_replication_; k++) {
                                string worker_address = pos->second.local_redistribute_addr_;
                                if (redistribution_map.find(worker_address) == redistribution_map.end())
                                    redistribution_map[worker_address] = new redistribution_address();
                                (*redistribution_map[worker_address])[target_address].insert(pair<string, bool>(key, false));
                                if (++pos == ebs_hash_ring.end()) pos = ebs_hash_ring.begin();
                            }
                        }
                    }
                }
                for (auto it = redistribution_map.begin(); it != redistribution_map.end(); it++) {
                    zmq_util::send_msg((void*)it->second, &cache[it->first]);
                }
            }
            else {
                cout << "Invalid Request\n";
            }*/
        }
    }
    for (auto& th: memory_threads) th.join();
    return 0;
}
