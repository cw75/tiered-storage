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
#define MEMORY_THREAD_NUM 1

// Define the number of ebs threads
#define EBS_THREAD_NUM 3

// Define local ebs replication factor
#define LOCAL_EBS_REPLICATION 2

// For simplicity, the kvs uses integer as the key type and maxintlattice as the value lattice.
typedef KV_Store<string, RC_KVS_PairLattice<string>> Database;

typedef consistent_hash_map<worker_node_t,ebs_hasher> ebs_hash_t;

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

struct changeset_data_wrapper {
    changeset_data_wrapper() : local(false) {}

    changeset_data c_data;
    bool local;
};

typedef unordered_map<string, unordered_set<string>> changeset_address;

typedef unordered_map<string, unordered_set<pair<string, bool>, pair_hash>> redistribution_address;

struct key_info {
    key_info(): tier_('E'), global_ebs_replication_(GLOBAL_EBS_REPLICATION), local_ebs_replication_(LOCAL_EBS_REPLICATION) {}
    key_info(char tier, int global_ebs_replication, int local_ebs_replication): tier_(tier), global_ebs_replication_(global_ebs_replication), local_ebs_replication_(local_ebs_replication) {}
    char tier_;
    int global_ebs_replication_;
    int local_ebs_replication_;
};

atomic<int> lww_timestamp(0);

bool enable_ebs(false);

pair<RC_KVS_PairLattice<string>, bool> process_get(string key, int thread_id) {
    RC_KVS_PairLattice<string> res;
    bool succeed = true;
    communication::Payload pl;
    string fname = "/home/ubuntu/ebs/ebs_" + to_string(thread_id) + "/" + key;
    fstream input(fname, ios::in | ios::binary);
    if (!input) {
      //cout << "File not found.  Creating a new file.\n";
      succeed = false;
    }
    else if (!pl.ParseFromIstream(&input)) {
      cerr << "Failed to parse payload." << endl;
      succeed = false;
    }
    else {
        res = RC_KVS_PairLattice<string>(timestamp_value_pair<string>(pl.timestamp(), pl.value()));
    }
    return pair<RC_KVS_PairLattice<string>, bool>(res, succeed);
}

bool process_put(string key, int timestamp, string value, int thread_id, unordered_set<string>& key_set) {
    bool succeed = true;
    timestamp_value_pair<string> p = timestamp_value_pair<string>(timestamp, value);
    communication::Payload pl_orig;
    communication::Payload pl;
    string fname = "/home/ubuntu/ebs/ebs_" + to_string(thread_id) + "/" + key;
    fstream input(fname, ios::in | ios::binary);
    if (!input) {
        //cout << "File not found.  Creating a new file.\n";
        pl.set_timestamp(timestamp);
        pl.set_value(value);
        fstream output(fname, ios::out | ios::trunc | ios::binary);
        if (!pl.SerializeToOstream(&output)) {
            cerr << "Failed to write payload\n";
            succeed = false;
        }
    }
    else if (!pl_orig.ParseFromIstream(&input)) {
        cerr << "Failed to parse payload." << endl;
        succeed = false;
    }
    else {
        RC_KVS_PairLattice<string> l = RC_KVS_PairLattice<string>(timestamp_value_pair<string>(pl_orig.timestamp(), pl_orig.value()));
        l.merge(p);
        pl.set_timestamp(l.reveal().timestamp);
        pl.set_value(l.reveal().value);
        fstream output(fname, ios::out | ios::trunc | ios::binary);
        if (!pl.SerializeToOstream(&output)) {
            cerr << "Failed to write payload\n";
            succeed = false;
        }
    }
    if (succeed)
        key_set.insert(key);
    return succeed;
}

// Handle request from clients
string process_client_request(communication::Request& req, int thread_id, unordered_set<string>& local_changeset, unordered_set<string>& key_set) {
    communication::Response response;

    if (req.has_get()) {
        cout << "received get by thread " << thread_id << "\n";
        auto res = process_get(req.get().key(), thread_id);
        response.set_succeed(res.second);
        response.set_value(res.first.reveal().value);
    }
    else if (req.has_put()) {
        cout << "received put by thread " << thread_id << "\n";
        response.set_succeed(process_put(req.put().key(), lww_timestamp.load(), req.put().value(), thread_id, key_set));
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
void process_distributed_gossip(communication::Gossip& gossip, int thread_id, unordered_set<string>& key_set) {
    for (int i = 0; i < gossip.tuple_size(); i++) {
        process_put(gossip.tuple(i).key(), gossip.tuple(i).timestamp(), gossip.tuple(i).value(), thread_id, key_set);
    }
}

// Handle local gossip from threads on the same node
void process_local_gossip(gossip_data* g_data, int thread_id, unordered_set<string>& key_set) {
    for (auto it = g_data->begin(); it != g_data->end(); it++) {
        process_put(it->first, it->second.reveal().timestamp, it->second.reveal().value, thread_id, key_set);
    }
    delete g_data;
}

void send_gossip(changeset_address* change_set_addr, SocketCache& cache, string ip, int thread_id) {
    unordered_map<string, gossip_data*> local_gossip_map;
    unordered_map<string, communication::Gossip> distributed_gossip_map;
    for (auto map_it = change_set_addr->begin(); map_it != change_set_addr->end(); map_it++) {
        vector<string> v;
        split(map_it->first, ':', v);
        // add to local gossip map
        if (v[0] == ip) {
            local_gossip_map[worker_node_t(ip, stoi(v[1])).local_gossip_addr_] = new gossip_data;
            for (auto set_it = map_it->second.begin(); set_it != map_it->second.end(); set_it++) {
                cout << "local gossip key: " + *set_it + " by thread " + to_string(thread_id) + "\n";
                auto res = process_get(*set_it, thread_id);
                local_gossip_map[worker_node_t(ip, stoi(v[1])).local_gossip_addr_]->emplace(*set_it, res.first);
            }
        }
        // add to distribute gossip map
        else {
            for (auto set_it = map_it->second.begin(); set_it != map_it->second.end(); set_it++) {
                cout << "distribute gossip key: " + *set_it + " by thread " + to_string(thread_id) + "\n";
                communication::Gossip_Tuple* tp = distributed_gossip_map[worker_node_t(v[0], stoi(v[1])).distributed_gossip_connect_addr_].add_tuple();
                tp->set_key(*set_it);
                auto res = process_get(*set_it, thread_id);
                tp->set_value(res.first.reveal().value);
                tp->set_timestamp(res.first.reveal().timestamp);
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

template<typename N, typename H>
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
}

/*void redistribute(unique_ptr<Database>& kvs, SocketCache& cache, global_hash_t& hash_ring, crc32_hasher& hasher, string ip, size_t port, node_t dest_node) {
    // perform local gossip
    if (ip == dest_node.ip_) {
        cout << "local redistribute \n";
        gossip_data* g_data = new gossip_data;
        unordered_set<string> keys = kvs->keys();
        unordered_set<string> to_remove;
        for (auto it = keys.begin(); it != keys.end(); it++) {
            if (!responsible(*it, hash_ring, hasher, ip, port)) {
                to_remove.insert(*it);
            }
            if (responsible(*it, hash_ring, hasher, dest_node.ip_, dest_node.port_)) {
                cout << "node " + ip + " thread " + to_string(port) + " moving " + *it + " with value " + kvs->get(*it).reveal().value + " to node " + dest_node.ip_ + " thread " + to_string(dest_node.port_) + "\n";
                g_data->emplace(*it, kvs->get(*it));
            }            
        }
        for (auto it = to_remove.begin(); it != to_remove.end(); it++) {
            kvs->remove(*it);
        }
        zmq_util::send_msg((void*)g_data, &cache[dest_node.lgossip_addr_]);
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
}*/

// ebs worker event loop
void ebs_worker_routine (zmq::context_t* context, string ip, int thread_id)
{
    size_t port = SERVER_PORT + thread_id;

    unordered_set<string> key_set;

    unordered_set<string> local_changeset;

    // initialize the thread's kvs replica
    unique_ptr<Database> kvs(new Database);

    // socket that respond to client requests
    zmq::socket_t responder(*context, ZMQ_REP);
    responder.bind(worker_node_t(ip, port).client_connection_bind_addr_);
    // socket that listens for distributed gossip
    zmq::socket_t dgossip_puller(*context, ZMQ_PULL);
    dgossip_puller.bind(worker_node_t(ip, port).distributed_gossip_bind_addr_);
    // socket that listens for local gossip
    zmq::socket_t lgossip_puller(*context, ZMQ_PULL);
    lgossip_puller.bind(worker_node_t(ip, port).local_gossip_addr_);
    // socket that listens for local gossip
    zmq::socket_t lredistribute_puller(*context, ZMQ_PULL);
    lredistribute_puller.bind(worker_node_t(ip, port).local_redistribute_addr_);
    // socket that listens for departure command
    zmq::socket_t depart_puller(*context, ZMQ_PULL);
    depart_puller.bind(worker_node_t(ip, port).local_depart_addr_);

    // used to communicate with master thread for changeset addresses
    zmq::socket_t changeset_address_requester(*context, ZMQ_REQ);
    changeset_address_requester.connect(master_node_t(ip).changeset_addr_);

    // used to send gossip
    SocketCache cache(context, ZMQ_PUSH);

    //  Initialize poll set
    vector<zmq::pollitem_t> pollitems = {
        { static_cast<void *>(responder), 0, ZMQ_POLLIN, 0 },
        { static_cast<void *>(dgossip_puller), 0, ZMQ_POLLIN, 0 },
        { static_cast<void *>(lgossip_puller), 0, ZMQ_POLLIN, 0 },
        { static_cast<void *>(lredistribute_puller), 0, ZMQ_POLLIN, 0 },
        { static_cast<void *>(depart_puller), 0, ZMQ_POLLIN, 0 },
        //{ static_cast<void *>(gossip_command_puller), 0, ZMQ_POLLIN, 0 }
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
            string result = process_client_request(req, thread_id, local_changeset, key_set);
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
            process_distributed_gossip(gossip, thread_id, key_set);
        }

        // If there is gossip from threads on the same node
        if (pollitems[2].revents & ZMQ_POLLIN) {
            cout << "received local gossip by thread " + to_string(thread_id) + "\n";
            //  Process local gossip
            zmq::message_t msg;
            zmq_util::recv_msg(&lgossip_puller, msg);
            gossip_data* g_data = *(gossip_data **)(msg.data());
            process_local_gossip(g_data, thread_id, key_set);
        }
        // If receives a local redistribute command
        if (pollitems[3].revents & ZMQ_POLLIN) {
            cout << "received local redistribute request by thread " + to_string(thread_id) + "\n";
            zmq::message_t msg;
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
            }
        }
        // If receives a departure command
        if (pollitems[4].revents & ZMQ_POLLIN) {
            cout << "THREAD " + to_string(thread_id) + " received departure command\n";
            string req = zmq_util::recv_string(&depart_puller);
            vector<string> v;
            split(req, ':', v);
            if (v[0] != "depart")
                cout << "error: not receiving depart\n";
            else {
                changeset_data_wrapper* data = new changeset_data_wrapper();
                data->local = true;
                data->c_data.first = port;
                for (auto it = key_set.begin(); it != key_set.end(); it++) {
                    (data->c_data.second).insert(*it);
                }
                zmq_util::send_msg((void*)data, &changeset_address_requester);
                zmq::message_t msg;
                zmq_util::recv_msg(&changeset_address_requester, msg);
                changeset_address* res = *(changeset_address **)(msg.data());
                send_gossip(res, cache, ip, thread_id);
                delete res;
                string shell_command;
                if (enable_ebs)
                    shell_command = "scripts/remove_volume.sh " + v[1] + " " + to_string(thread_id);
                else
                    shell_command = "scripts/remove_volume_dummy.sh " + v[1] + " " + to_string(thread_id);
                system(shell_command.c_str());
                zmq_util::send_string(v[1] + ":" + to_string(thread_id), &cache[master_node_t(ip).local_depart_done_addr_]);
                break;
            }
        }
        // If receives a gossip command from the master thread
        //if (pollitems[4].revents & ZMQ_POLLIN) {
            // TODO: gossip
        //}

        end = std::chrono::system_clock::now();
        if (chrono::duration_cast<std::chrono::seconds>(end-start).count() >= PERIOD || local_changeset.size() >= THRESHOLD) {
            if (local_changeset.size() >= THRESHOLD)
                cout << "reached gossip threshold\n";
            if (local_changeset.size() > 0) {
                changeset_data_wrapper* data = new changeset_data_wrapper();
                data->c_data.first = port;
                for (auto it = local_changeset.begin(); it != local_changeset.end(); it++) {
                    (data->c_data.second).insert(*it);
                }
                zmq_util::send_msg((void*)data, &changeset_address_requester);
                zmq::message_t msg;
                zmq_util::recv_msg(&changeset_address_requester, msg);
                changeset_address* res = *(changeset_address **)(msg.data());
                send_gossip(res, cache, ip, thread_id);
                delete res;
                local_changeset.clear();
            }
            start = std::chrono::system_clock::now();
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        cerr << "usage:" << argv[0] << " <new_node> <enable_ebs>" << endl;
        return 1;
    }
    if (string(argv[1]) != "y" && string(argv[1]) != "n") {
        cerr << "invalid argument" << endl;
        return 1;
    }

    //std::cout << GetCurrentWorkingDir() << std::endl;

    string ip = getIP();
    string new_node = argv[1];
    if (string(argv[2]) == "y")
        enable_ebs = true;
    else
        enable_ebs = false;
    //  Prepare our context
    zmq::context_t context(1);

    SocketCache cache(&context, ZMQ_PUSH);

    SocketCache key_address_requesters(&context, ZMQ_REQ);

    global_hash_t global_hash_ring;
    ebs_hash_t ebs_hash_ring;

    unordered_map<string, key_info> placement;

    //unordered_map<string, unordered_map<string, unordered_set<string>>> change_map;

    set<int> active_ebs_thread_id = set<int>();
    for (int i = 1; i <= EBS_THREAD_NUM; i++) {
        active_ebs_thread_id.insert(i);
    }

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

    map<string, int> ebs_device_map;
    unordered_map<int, string> inverse_ebs_device_map;

    vector<thread> ebs_threads;
    string eid = "ba";

    for (int thread_id = 1; thread_id <= EBS_THREAD_NUM; thread_id++) {
        ebs_device_map[eid] = thread_id;
        inverse_ebs_device_map[thread_id] = eid;
        cout << "device id to be added is " + eid + "\n";
        cout << "thread id is " + to_string(thread_id) + "\n";
        string shell_command;
        if (enable_ebs)
            shell_command = "scripts/add_volume.sh " + eid + " 10 " + to_string(thread_id);
        else
            shell_command = "scripts/add_volume_dummy.sh " + eid + " 10 " + to_string(thread_id);
        system(shell_command.c_str());
        eid = getNextDeviceID(eid);
        ebs_threads.push_back(thread(ebs_worker_routine, &context, ip, thread_id));
        ebs_hash_ring.insert(worker_node_t(ip, SERVER_PORT + thread_id));
    }

    set<int> active_thread_id = set<int>();
    for (int i = 1; i <= EBS_THREAD_NUM; i++) {
        active_thread_id.insert(i);
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
    // responsible for pulling depart done msg from workers
    zmq::socket_t depart_done_puller(context, ZMQ_PULL);
    depart_done_puller.bind(master_node_t(ip).local_depart_done_addr_);

    zmq_pollitem_t pollitems [7];
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
    pollitems[5].socket = static_cast<void *>(depart_done_puller);
    pollitems[5].events = ZMQ_POLLIN;
    pollitems[6].socket = NULL;
    pollitems[6].fd = 0;
    pollitems[6].events = ZMQ_POLLIN;

    string input;
    int next_thread_id = EBS_THREAD_NUM + 1;
    while (true) {
        zmq::poll(pollitems, 7, -1);
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
            string new_server_ip = zmq_util::recv_string(&join_puller);
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
            }
        }
        else if (pollitems[2].revents & ZMQ_POLLIN) {
            cout << "received departure of other nodes\n";
            string departing_server_ip = zmq_util::recv_string(&depart_puller);
            // update hash ring
            global_hash_ring.erase(master_node_t(departing_server_ip));
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
                // for now, just use EBS as tier and LOCAL_EBS_REPLICATION as rep factor
                // update placement metadata
                if (placement.find(key) == placement.end())
                    placement[key] = key_info('E', GLOBAL_EBS_REPLICATION, LOCAL_EBS_REPLICATION);
                // for now, randomly select a valid worker address for the client
                vector<worker_node_t> ebs_worker_nodes;
                // use hash ring to find the right node to contact
                auto it = ebs_hash_ring.find(key);
                for (int i = 0; i < placement[key].local_ebs_replication_; i++) {
                    ebs_worker_nodes.push_back(it->second);
                    if (++it == ebs_hash_ring.end()) it = ebs_hash_ring.begin();
                }
                string worker_address = ebs_worker_nodes[rand()%ebs_worker_nodes.size()].client_connection_connect_addr_;
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
                for (int i = 0; i < req.tuple_size(); i++) {
                    communication::Key_Response_Tuple* tp = res.add_tuple();
                    string key = req.tuple(i).key();
                    tp->set_key(key);
                    cout << "key requested is " + key + "\n";
                    // for now, just use EBS as tier and LOCAL_EBS_REPLICATION as rep factor
                    // update placement metadata
                    if (placement.find(key) == placement.end())
                        placement[key] = key_info('E', GLOBAL_EBS_REPLICATION, LOCAL_EBS_REPLICATION);
                    auto it = ebs_hash_ring.find(key);
                    for (int i = 0; i < placement[key].local_ebs_replication_; i++) {
                        communication::Key_Response_Address* tp_addr = tp->add_address();
                        tp_addr->set_addr(it->second.id_);
                        if (++it == ebs_hash_ring.end()) it = ebs_hash_ring.begin();
                    }
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
            changeset_data_wrapper* data = *(changeset_data_wrapper **)(msg.data());
            string self_id = ip + ":" + to_string(data->c_data.first);
            changeset_address* res = new changeset_address();
            unordered_map<master_node_t, unordered_set<string>, node_hash> node_map;
            for (auto it = data->c_data.second.begin(); it != data->c_data.second.end(); it++) {
                string key = *it;
                // first, check the local ebs ring
                auto ebs_pos = ebs_hash_ring.find(key);
                for (int i = 0; i < placement[key].local_ebs_replication_; i++) {
                    if (ebs_pos->second.id_.compare(self_id) != 0) {
                        (*res)[ebs_pos->second.id_].insert(key);
                    }
                    if (++ebs_pos == ebs_hash_ring.end()) ebs_pos = ebs_hash_ring.begin();
                }
                // second, check the global ring and request worker addresses from other node's master thread
                cout << "local flag is " + to_string(data->local) + "\n";
                if (!data->local) {
                    auto pos = global_hash_ring.find(key);
                    for (int i = 0; i < placement[key].global_ebs_replication_; i++) {
                        if (pos->second.ip_.compare(ip) != 0) {
                            node_map[pos->second].insert(key);
                        }
                        if (++pos == global_hash_ring.end()) pos = global_hash_ring.begin();
                    }
                }
            }
            if (!data->local) {
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
            }
            zmq_util::send_msg((void*)res, &changeset_address_responder);
            delete data;
        }
        else if (pollitems[5].revents & ZMQ_POLLIN) {
            cout << "received depart done msg\n";
            vector<string> v;
            split(zmq_util::recv_string(&depart_done_puller), ':', v);
            ebs_device_map[v[0]] = -1;
            inverse_ebs_device_map.erase(stoi(v[1]));
        }
        else {
            getline(cin, input);
            if (input == "ADD") {
                cout << "adding ebs thread\n";
                string ebs_device_id;
                if (ebs_device_map.size() == 0) {
                    ebs_device_id = "ba";
                    ebs_device_map[ebs_device_id] = next_thread_id;
                    inverse_ebs_device_map[next_thread_id] = ebs_device_id;
                }
                else {
                    bool has_slot = false;
                    cout << "start iterating the ebs device map\n";
                    for (auto it = ebs_device_map.begin(); it != ebs_device_map.end(); it++) {
                        cout << "key " + it->first + " maps to thread id " + to_string(it->second) + "\n";
                        if (it->second == -1) {
                            has_slot = true;
                            ebs_device_id = it->first;
                            ebs_device_map[it->first] = next_thread_id;
                            inverse_ebs_device_map[next_thread_id] = it->first;
                            break;
                        }
                    }
                    if(!has_slot) {
                        ebs_device_id = getNextDeviceID((ebs_device_map.rbegin())->first);
                        ebs_device_map[ebs_device_id] = next_thread_id;
                        inverse_ebs_device_map[next_thread_id] = ebs_device_id;
                    }
                }
                cout << "device id to be added is " + ebs_device_id + "\n";
                cout << "thread id is " + to_string(next_thread_id) + "\n";
                string shell_command;
                if (enable_ebs)
                    shell_command = "scripts/add_volume.sh " + ebs_device_id + " 10 " + to_string(next_thread_id);
                else
                    shell_command = "scripts/add_volume_dummy.sh " + ebs_device_id + " 10 " + to_string(next_thread_id);
                system(shell_command.c_str());
                ebs_threads.push_back(thread(ebs_worker_routine, &context, ip, next_thread_id));
                active_thread_id.insert(next_thread_id);
                ebs_hash_ring.insert(worker_node_t(ip, SERVER_PORT + next_thread_id));
                // repartition data
                unordered_map<string, redistribution_address*> redistribution_map;
                for (auto it = placement.begin(); it != placement.end(); it++) {
                    worker_node_t sender_node;
                    bool remove = false;
                    cout << "examining key " + it->first + "\n";
                    bool resp = responsible<worker_node_t, ebs_hasher>(it->first, it->second.local_ebs_replication_, ebs_hash_ring, ip, (SERVER_PORT + next_thread_id), sender_node, remove);
                    if (resp) {
                        cout << "the new thread is responsible for key " + it->first + "\n";
                        string sender_address = sender_node.local_redistribute_addr_;
                        cout << "sender address is " + sender_address + "\n";
                        string target_address = ip + ":" + to_string(SERVER_PORT + next_thread_id);
                        if (redistribution_map.find(sender_address) == redistribution_map.end())
                            redistribution_map[sender_address] = new redistribution_address();
                        (*redistribution_map[sender_address])[target_address].insert(pair<string, bool>(it->first, remove));
                    }
                }
                for (auto it = redistribution_map.begin(); it != redistribution_map.end(); it++) {
                    zmq_util::send_msg((void*)it->second, &cache[it->first]);
                }

                next_thread_id += 1;
            }
            else if (input == "REMOVE") {
                if (active_thread_id.rbegin() == active_thread_id.rend())
                    cout << "error: no thread left to remove\n";
                else {
                    int target_thread_id = *(active_thread_id.rbegin());
                    cout << "removing thread " + to_string(target_thread_id) + "\n";
                    ebs_hash_ring.erase(worker_node_t(ip, SERVER_PORT + target_thread_id));
                    active_thread_id.erase(target_thread_id);
                    string device_id = inverse_ebs_device_map[target_thread_id];
                    zmq_util::send_string("depart:" + device_id, &cache[worker_node_t(ip, SERVER_PORT + target_thread_id).local_depart_addr_]);
                }
            }
            else if (input == "DEPART") {
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
            else if (input == "KILL") {
                cout << "killing all ebs instances\n";
                for (auto it = inverse_ebs_device_map.begin(); it != inverse_ebs_device_map.end(); it++) {
                    active_thread_id.erase(it->first);
                    ebs_hash_ring.erase(worker_node_t(ip, SERVER_PORT + it->first));
                    string shell_command;
                    if (enable_ebs)
                        shell_command = "scripts/remove_volume.sh " + it->second + " " + to_string(it->first);
                    else
                        shell_command = "scripts/remove_volume_dummy.sh " + it->second + " " + to_string(it->first);
                    system(shell_command.c_str());
                }
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
    for (auto& th: ebs_threads) th.join();
    return 0;
}
