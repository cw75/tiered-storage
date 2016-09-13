#include <pthread.h>
#include <unistd.h>

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <zmq.hpp>

#include "kv_store/ruc_kvs/message.pb.h"
#include "kv_store/include/rc_kv_store.h"

using namespace std;

// NOTE(mwhittaker): I'm not very familiar with ZeroMQ, so this code was a bit
// challenging for me to read. I just want to double check that I'm
// understanding things correctly.
//
// A message queue (msgqueue.cpp) can connect any number of clients to any
// number of servers:
//
//     clients                     servers
//     =======                     =======
//
//     +---+                       +---+
//     | a |\                     /| 1 |
//     +---+ \                   / +---+
//            \                 /
//     +---+   \ +----------+  /   +---+
//     | b |---->| msgqueue |<-----| 2 |
//     +---+   / +----------+  \   +---+
//            /                 \
//     +---+ /                   \ +---+
//     | c |/                     \| 3 |
//     +---+                       +---+
//
// Messages sent by clients are fairly queued and sent in a round-robin fashion
// to the servers. Here, for example, it would send to 1, then 2, then 3, then
// 1, then 2, and so on. When a server receives a message, it can respond with
// a message that is routed back to the original sender.
//
// In our case, we only support a single server:
//
//     clients                     server
//     =======                     ======
//
//     +---+
//     | a |\
//     +---+ \
//            \
//     +---+   \ +----------+      +---+
//     | b |---->| msgqueue |<-----| 1 |
//     +---+   / +----------+      +---+
//            /
//     +---+ /
//     | c |/
//     +---+
//
// This server runs some number of threads, and each thread maintains a copy of
// a key-value store as represented by a map from integers to timestamped
// integers; this happens to be a semilattice. For example,
//
//   {
//      // key: (timestamp, value)
//      139911: (42, 100),
//      13941: (1, 14703),
//      ...
//   }
//
// Each thread forms a connection to the message queue so that it can receive
// messages from clients. It also forms a clique of pub-sub connections with
// the other threads in order to gossip updates to the key-value store. These
// pub-sub connections are made within the process and pass data around via
// pointers to data allocated on the heap.
//
// A couple questions:
// - If one client begins a transaction, reaches thread 1, and writes a value.
//   Can't it then issue a read, reach thread 2, and not see the value there?
//   Is this read uncommited? I don't mean to imply it's not; I truly don't
//   know.
// - What's the point of the message queue if we can only ever have a single
//   server? Is the single-server thing a short term convenience?
// - What is the purpose of the timestamps?

// NOTE(mwhittaker): Prefer constants to #defines!
// Define the number of threads
#define THREAD_NUM 4

// NOTE(mwhittaker): Prefer constants to #defines!
// If the total number of updates to the kvs before the last gossip reaches THRESHOLD, then the thread gossips to others.
#define THRESHOLD 1

// NOTE(mwhittaker): Consider a using type alias?
// http://en.cppreference.com/w/cpp/language/type_alias
// For simplicity, the kvs uses integer as the key type and maxintlattice as the value lattice.
typedef KV_Store<string, RC_KVS_PairLattice<string>> Database;

atomic<int> bind_successful {0};

struct coordination_data{
    unordered_map<string, RC_KVS_PairLattice<string>> data;
    atomic<int> processed {0};
};

// This function performs the actual request processing
string process_request(unique_ptr<Database> &kvs, communication::Request &req, int &update_counter, SetLattice<string> &change_set, int &local_timestamp, int thread_id) {
    communication::Response response;

    if (req.type() == "BEGIN TRANSACTION") {
        // NOTE(mwhittaker): Why local_timestamp + thread_id?
        response.set_timestamp(stoi(to_string(local_timestamp++) + to_string(thread_id)));
    }
    // else if (req.type() == "END TRANSACTION") {
    // }
    else if (req.type() == "GET") {
        response.set_value(kvs->get(req.key()).reveal().value);
        response.set_succeed(true);
    }
    else if (req.type() == "PUT") {
        //cout << "value to be put is " << req.value() << "\n";
        change_set.insert(req.key());
        timestamp_value_pair<string> p = timestamp_value_pair<string>(req.timestamp(), req.value());
        kvs->put(req.key(), p);
        response.set_succeed(true);
        update_counter++;
    }
    else {
        response.set_err(true);
        response.set_succeed(false);
    }
    string data;
    response.SerializeToString(&data);
    return data;
}

void send_gossip(SetLattice<string> &change_set, unique_ptr<Database> &kvs, zmq::socket_t &publisher){
    coordination_data *c_data = new coordination_data;
    for (auto it = change_set.reveal().begin(); it != change_set.reveal().end(); it++) {
        c_data->data.emplace(*it, kvs->get(*it));
    }

    zmq_msg_t msg;
    zmq_msg_init_size(&msg, sizeof(coordination_data**));
    memcpy(zmq_msg_data(&msg), &c_data, sizeof(coordination_data**));
    zmq_msg_send(&msg, static_cast<void *>(publisher), 0);
}

void receive_gossip(unique_ptr<Database> &kvs, zmq::socket_t &subscriber, int thread_id){
    zmq_msg_t rec;
    zmq_msg_init(&rec);
    //cout << "entering gossip reception routine\n";
    zmq_msg_recv(&rec, static_cast<void *>(subscriber), 0);
    coordination_data *c_data = *(coordination_data **)zmq_msg_data(&rec);
    zmq_msg_close(&rec);
    //cout << "The gossip is received by thread " << thread_id << "\n";
    // merge delta from other threads
    for (auto it = c_data->data.begin(); it != c_data->data.end(); it++) {
        kvs->put(it->first, it->second);
    }

    if (c_data->processed.fetch_add(1) == THREAD_NUM - 1 - 1) {
        delete c_data;
        //cout << "The gossip is successfully garbage collected by thread " << thread_id << "\n";
    }
}

// Act as an event loop for the server
void *worker_routine (zmq::context_t *context, int thread_id)
{
    int local_timestamp = 0;
    // initialize the thread's kvs replica
    unique_ptr<Database> kvs(new Database);

    // initialize a set lattice that keeps track of the keys that get updated
    unique_ptr<SetLattice<string>> change_set(new SetLattice<string>);

    zmq::socket_t responder (*context, ZMQ_REP);
    responder.connect ("tcp://localhost:5560");

    zmq::socket_t publisher (*context, ZMQ_PUB);
    publisher.bind("inproc://" + to_string(thread_id));
    bind_successful++;

    zmq::socket_t subscriber (*context, ZMQ_SUB);

    while (bind_successful.load() != THREAD_NUM) {}

    for (int i = 0; i < THREAD_NUM; i++) {
        if (i != thread_id) {
            subscriber.connect("inproc://" + to_string(i));
        }
    }
    const char *filter = "";
    subscriber.setsockopt(ZMQ_SUBSCRIBE, filter, strlen (filter));

    //  Initialize poll set
    zmq::pollitem_t items [] = {
        { static_cast<void *>(responder), 0, ZMQ_POLLIN, 0 },
        { static_cast<void *>(subscriber), 0, ZMQ_POLLIN, 0 }
    };

    // A counter that keep track of the number of updates performed to the kvs before the last gossip
    int update_counter = 0;

    // Enter the event loop
    while (true) {
        // this_thread::sleep_for(std::chrono::microseconds(500));
        // zmq_msg_t rec;
        // zmq_msg_init(&rec);
        zmq::poll (&items [0], 2, -1);

        // If there is a request from clients
        if (items [0].revents & ZMQ_POLLIN) {
            zmq_msg_t rec;
            zmq_msg_init(&rec);
            //cout << "entering request handling routine\n";
            zmq_msg_recv(&rec, static_cast<void *>(responder), 0);
            string data = (char *)zmq_msg_data(&rec);
            zmq_msg_close(&rec);
            communication::Request req;
            req.ParseFromString(data);

            //cout << "Received request: " << req.type() << " on thread " << thread_id << "\n";
            //  Process request
            string result = process_request(kvs, req, update_counter, *change_set, local_timestamp, thread_id);
            //  Send reply back to client
            zmq_msg_t msg;
            zmq_msg_init_size(&msg, result.size());
            memcpy(zmq_msg_data(&msg), &(result[0]), result.size());
            zmq_msg_send(&msg, static_cast<void *>(responder), 0);
        }

        // If there is a message from other threads
        if (items [1].revents & ZMQ_POLLIN) {
            receive_gossip(kvs, subscriber, thread_id);
        }

        if (update_counter >= THRESHOLD && THREAD_NUM != 1) {
            send_gossip(*change_set, kvs, publisher);

            // Reset the change_set and update_counter
            change_set.reset(new SetLattice<string>);
            update_counter = 0;
            //cout << "The gossip is sent by thread " << thread_id << "\n";
        }
    }
    return (NULL);
}

int main ()
{
    //  Prepare our context
    zmq::context_t context (1);

    //  Launch pool of worker threads
    //cout << "Starting the server with " << THREAD_NUM << " threads and gossip threshold " << THRESHOLD << "\n";

    vector<thread> threads;
    for (int thread_id = 0; thread_id != THREAD_NUM; thread_id++) {
        threads.push_back(thread(worker_routine, &context, thread_id));
    }
    for (auto& th: threads) th.join();
    return 0;
}
