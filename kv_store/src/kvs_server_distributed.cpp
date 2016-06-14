#include <zmq.hpp>
#include <string>
#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include <memory>
#include "kv_store.h"
#include "request.pb.h"
#include "response.pb.h"

using namespace std;

string process_request(unique_ptr<KV_Store> &kvs, communication::Request &req) {
    //cout << "value is " << req.value() << "\n";
    communication::Response response;
    if (req.type() == "GET") {
        version_value_pair result = kvs->get(req.key());
        response.set_value(result.value.reveal());
        auto version_vector = result.v_map.reveal();
        for (auto it = version_vector.begin(); it != version_vector.end(); ++it) {
            (*response.mutable_version_vector())[it->first] = it->second.reveal();
        }
        response.set_succeed(true);
    }
    else if (req.type() == "PUT") {
        unordered_map<int, MaxLattice<int>> m;
        auto v = req.version_vector();
        for (auto it = v.begin(); it != v.end(); ++it) {
            cout << "vector is " << it->first << ": " << it->second << "\n";
            m.emplace(it->first, MaxLattice<int>(it->second));
        }
        version_value_pair p = version_value_pair(MapLattice<int, MaxLattice<int>>(m), MaxLattice<int>(req.value()));
        kvs->put(req.key(), p);
        cout << "verifying " << kvs->get(req.key()).value.reveal() << "\n";
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

void *worker_routine (void *arg)
{
    unique_ptr<KV_Store> kvs(new KV_Store);

    zmq::context_t *context = (zmq::context_t *) arg;

    zmq::socket_t responder (*context, ZMQ_REP);
    responder.connect ("tcp://localhost:5560");

    zmq::socket_t publisher (*context, ZMQ_PUB);
    publisher.connect("inproc://input");

    zmq::socket_t subscriber (*context, ZMQ_SUB);
    subscriber.connect("inproc://output");
    const char *filter = "";
    subscriber.setsockopt(ZMQ_SUBSCRIBE, filter, strlen (filter));

    //  Initialize poll set
    zmq::pollitem_t items [] = {
        { static_cast<void *>(responder), 0, ZMQ_POLLIN, 0 },
        { static_cast<void *>(subscriber), 0, ZMQ_POLLIN, 0 }
    };

    int counter = 0;

    while (true) {
        zmq_msg_t rec;
        zmq_msg_init(&rec);
        zmq::poll (&items [0], 2, -1);

        if (items [0].revents & ZMQ_POLLIN) {
            zmq_msg_recv(&rec, static_cast<void *>(responder), 0);
            string data = (char *)zmq_msg_data(&rec);
            zmq_msg_close(&rec);
            communication::Request req;
            req.ParseFromString(data);

            cout << "Received request: " << req.type() << " by thread " << pthread_self() << "\n";
            //  Process request
            string result = process_request(kvs, req);
            //  Send reply back to client
            zmq_msg_t msg;
            zmq_msg_init_size(&msg, result.size());
            memcpy(zmq_msg_data(&msg), &(result[0]), result.size());
            zmq_msg_send(&msg, static_cast<void *>(responder), 0);
            counter++;
            if (counter % 5 == 0) {
                zmq_msg_t msg;
                zmq_msg_init_size(&msg, 6);
                memcpy(zmq_msg_data(&msg), "Hello", 6);
                zmq_msg_send(&msg, static_cast<void *>(publisher), 0);
            }
        }

        if (items [1].revents & ZMQ_POLLIN) {
            zmq_msg_recv(&rec, static_cast<void *>(subscriber), 0);
            string data = (char *)zmq_msg_data(&rec);
            zmq_msg_close(&rec);
            cout << "Received message " << data << "\n";
        }
    }
    return (NULL);
}

int main ()
{
    //  Prepare our context and sockets
    zmq::context_t context (1);
    zmq::socket_t pub_subscriber (context, ZMQ_XSUB);
    pub_subscriber.bind ("inproc://input");
    // const char *filter = "H";
    // pub_subscriber.setsockopt(ZMQ_SUBSCRIBE, filter, strlen (filter));
    zmq::socket_t sub_publisher (context, ZMQ_XPUB);
    sub_publisher.bind ("inproc://output");

    //  Launch pool of worker threads
    for (int thread_nbr = 0; thread_nbr != 1; thread_nbr++) {
        pthread_t worker;
        pthread_create (&worker, NULL, worker_routine, (void *) &context);
    }
    //  Connect work threads to client threads via a queue
    zmq::proxy (static_cast<void *>(pub_subscriber), static_cast<void *>(sub_publisher), nullptr);
    return 0;
}