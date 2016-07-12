#include <zmq.hpp>
#include <string>
#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include <memory>
#include <vector>
#include <thread>
#include "versioned_kv_store.h"
#include "request.pb.h"
#include "response.pb.h"

using namespace std;

// Define the number of threads
#define THREAD_NUM 8

// If the total number of updates to the kvs before the last gossip reaches THRESHOLD, then the thread gossips to others.
#define THRESHOLD 20

// For simplicity, the kvs uses integer as the key type and maxintlattice as the value lattice.
typedef KV_Store<string, KVS_PairLattice<MaxLattice<int>>> Database;

atomic<int> bind_successful {0};

struct coordination_data{
    unordered_map<string, KVS_PairLattice<MaxLattice<int>>> data;
    atomic<int> processed {0};
};

// This function performs the actual request processing
string process_request(unique_ptr<Database> &kvs, communication::Request &req, int &update_counter, SetLattice<string> &change_set) {
    communication::Response response;
    if (req.type() == "GET") {
        version_value_pair<MaxLattice<int>> result = kvs->get(req.key()).reveal();
        response.set_value(result.value.reveal());
        auto version_vector = result.v_map.reveal();
        for (auto it = version_vector.begin(); it != version_vector.end(); ++it) {
            (*response.mutable_version_vector())[it->first] = it->second.reveal();
        }
        response.set_succeed(true);
    }
    else if (req.type() == "PUT") {
        //cout << "value to be put is " << req.value() << "\n";
        change_set.insert(req.key());

        unordered_map<int, MaxLattice<int>> m;
        auto v = req.version_vector();
        for (auto it = v.begin(); it != v.end(); ++it) {
            //cout << "vector is " << it->first << ": " << it->second << "\n";
            m.emplace(it->first, MaxLattice<int>(it->second));
        }
        version_value_pair<MaxLattice<int>> p = version_value_pair<MaxLattice<int>>(MapLattice<int, MaxLattice<int>>(m), MaxLattice<int>(req.value()));
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

// Act as an event loop for the server
void *worker_routine (zmq::context_t *context, int thread_id)
{
    // initialize the thread's kvs replica
    unique_ptr<Database> kvs(new Database);

    // initializa a set lattice that keeps track of the keys that get updated
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
            string result = process_request(kvs, req, update_counter, *change_set);
            //  Send reply back to client
            zmq_msg_t msg;
            zmq_msg_init_size(&msg, result.size());
            memcpy(zmq_msg_data(&msg), &(result[0]), result.size());
            zmq_msg_send(&msg, static_cast<void *>(responder), 0);
            if (update_counter == THRESHOLD && THREAD_NUM != 1) {
                coordination_data *c_data = new coordination_data;
                for (auto it = change_set->reveal().begin(); it != change_set->reveal().end(); it++) {
                    c_data->data.emplace(*it, kvs->get(*it));
                }

                zmq_msg_t msg;
                zmq_msg_init_size(&msg, sizeof(coordination_data**));
                memcpy(zmq_msg_data(&msg), &c_data, sizeof(coordination_data**));
                zmq_msg_send(&msg, static_cast<void *>(publisher), 0);

                // Reset the change_set and update_counter
                change_set.reset(new SetLattice<string>);
                update_counter = 0;
                //cout << "The gossip is sent by thread " << thread_id << "\n";
            }
        }

        // If there is a message from other threads
        if (items [1].revents & ZMQ_POLLIN) {
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