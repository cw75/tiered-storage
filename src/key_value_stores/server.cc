// Key-value server. See README.md for a high level overview!

#include <atomic>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <zmq.hpp>

#include "key_value_stores/barrier.h"
#include "key_value_stores/message.pb.h"
#include "key_value_stores/util.h"
#include "key_value_stores/zmq_util.h"
#include "lattices/map_lattice.h"
#include "lattices/max_lattice.h"
#include "lattices/set_union_lattice.h"
#include "lattices/timestamp_lattice.h"

namespace lf = latticeflow;

// The number of server threads.
constexpr int NUM_THREADS = 4;

// If the total number of updates to the key-value store since the last gossip
// reaches THRESHOLD, then the threads gossip with one another.
constexpr int THRESHOLD = 1;

// Mutex used to allow threads to print to stdout without clobbering each other.
std::mutex stdout_mutex;

// Our key-value store is a map from strings to timestamped strings.
using Timestamp = lf::MaxLattice<int>;
using MaxStringLattice = lf::MaxLattice<std::string>;
using TimestampedStringLattice =
    lf::TimestampLattice<Timestamp, MaxStringLattice>;
using Database = lf::MapLattice<std::string, TimestampedStringLattice>;

// Threads send GossipData messages to one another during gossip.
struct GossipData {
  GossipData() : num_processed(0) {}

  // The set of all database changes since the last gossip. Whenever a thread
  // receives a GossipData, it merges `db` into its local copy of the database.
  Database db;

  // The number of threads that have successfully processed this GossipData.
  std::atomic<int> num_processed;
};

// Processes a request from the user and returns a response that should be sent
// to the user. `thread_id` is the id of the currently running thread; `db` is
// the thread's copy of the database; `update_counter` counts the number of
// database updates; `change_set` records the set of keys that have been
// updated in the database; and `local_timestamp` is the local timestamp of the
// thread.
std::string process_request(communication::Request& request,
                            const int thread_id, Database* const db,
                            int* const update_counter,
                            std::set<std::string>* const change_set,
                            int* const local_timestamp) {
  communication::Response response;

  switch (request.request_case()) {
    case communication::Request::kBeginTransaction: {
      // TODO(mwhittaker): Why local_timestamp + thread_id?
      response.set_timestamp(std::stoi(std::to_string(*local_timestamp) +
                                       std::to_string(thread_id)));
      (*local_timestamp)++;
      break;
    }
    case communication::Request::kGet: {
      const TimestampedStringLattice& l = db->get(request.get().key());
      response.set_value(l.value().get());
      response.set_timestamp(l.timestamp().get());
      response.set_succeed(true);
      break;
    }
    case communication::Request::kPut: {
      for (const communication::Request::Put::KeyValuePair& kv_pair :
           request.put().kv_pair()) {
        change_set->insert(kv_pair.key());
        TimestampedStringLattice p(
            lf::MaxLattice<int>(request.put().timestamp()),
            lf::MaxLattice<std::string>(kv_pair.value()));
        db->put(kv_pair.key(), p);
        (*update_counter)++;
      }
      response.set_succeed(true);
      break;
    }
    case communication::Request::REQUEST_NOT_SET: {
      response.set_err(true);
      response.set_succeed(false);
      break;
    }
  }
  std::string response_str;
  response.SerializeToString(&response_str);
  return response_str;
}

// Given a database (`db`) and set of keys that have been updated in the
// database (`change_set`), send a GossipData message over `publisher`.
void send_gossip(const Database& db, const std::set<std::string> change_set,
                 zmq::socket_t* publisher) {
  // GossipData messages are shared on the heap. When a thread goes to publish
  // a GossipData message, it allocates in on the heap and sends a pointer to
  // the data to the other threads. Later, the last thread to process the
  // update, makes sure to free the memory.
  //
  // TODO(mwhittaker): Should we just serialize things rather than passing
  // pointers around? We'll have to do that anyway when we shift to
  // inter-server gossip.
  auto* gossip_data = new GossipData;
  for (const std::string& key : change_set) {
    gossip_data->db.put(key, db.get(key));
  }
  send_pointer(gossip_data, publisher);
}

// Receive and process a GossipData message over `subscriber` and update `db`
// accordingly.
void receive_gossip(Database* db, zmq::socket_t* subscriber) {
  GossipData* gossip_data = recv_pointer<GossipData>(subscriber);
  db->join(gossip_data->db);
  // See `send_gossip`.
  if (gossip_data->num_processed.fetch_add(1) == NUM_THREADS - 1) {
    delete gossip_data;
  }
}

// The main event loop of each thread. This thread is assigned thread id
// `thread_id`; uses the `all_threads_bound` barrier to ensure all threads have
// established their connections; and uses `context` to form connections to
// clients and other threads.
void worker_routine(const int thread_id, Barrier* all_threads_bound,
                    zmq::context_t* context) {
  // The local timestamp of the server. This is updated when clients begin new
  // transactions.
  int local_timestamp = 0;

  // This threads local copy of the database.
  Database kvs;

  // The keys that have been updated in the database since the last gossip.
  std::set<std::string> change_set;

  // The number of updates to the key value store since the last gossip.
  int update_counter = 0;

  // Socket connected to clients (technically, the message broker).
  zmq::socket_t responder(*context, ZMQ_REP);
  responder.connect("tcp://localhost:5560");
  {
    std::unique_lock<std::mutex> lock(stdout_mutex);
    std::cout << "thread " << thread_id << " connected to tcp://localhost:5560"
              << std::endl;
  }

  // Socket connected to other threads used for gossiping.
  zmq::socket_t publisher(*context, ZMQ_PUB);
  publisher.bind("inproc://" + std::to_string(thread_id));

  // All threads wait for all other threads to create their publisher socket.
  // TODO(mwhittaker): Do we need to do this? Doesn't ZeroMQ allow us to do
  // this in any order?
  all_threads_bound->enter();

  // Subscribe to other threads.
  zmq::socket_t subscriber(*context, ZMQ_SUB);
  for (int i = 0; i < NUM_THREADS; i++) {
    if (i != thread_id) {
      subscriber.connect("inproc://" + std::to_string(i));
    }
  }
  const char* filter = "";
  subscriber.setsockopt(ZMQ_SUBSCRIBE, filter, strlen(filter));

  // Threads listen on the client-facing socket and the gossiping socket at the
  // same time. We use ZeroMQ's polling mechanism to do this.
  zmq::pollitem_t items[] = {
      {static_cast<void*>(responder), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(subscriber), 0, ZMQ_POLLIN, 0}};

  // Enter the event loop!
  while (true) {
    zmq::poll(&items[0], 2 /* nitems */, -1 /* timeout */);

    // Process a request from the client.
    if (static_cast<bool>(items[0].revents & ZMQ_POLLIN)) {
      communication::Request request;
      recv_proto(&request, &responder);

      std::string result =
          process_request(request, thread_id, &kvs, &update_counter,
                          &change_set, &local_timestamp);
      send_string(result, &responder);
    }

    // Process a gossip message from other threads.
    if (static_cast<bool>(items[1].revents & ZMQ_POLLIN)) {
      receive_gossip(&kvs, &subscriber);
    }

    // Gossip to other threads.
    if (update_counter >= THRESHOLD && NUM_THREADS != 1) {
      send_gossip(kvs, change_set, &publisher);
      change_set.clear();
      update_counter = 0;
    }
  }
}

int main() {
  Barrier all_threads_bound(NUM_THREADS);
  zmq::context_t context(1);
  std::vector<std::thread> threads;
  for (int thread_id = 0; thread_id < NUM_THREADS; ++thread_id) {
    threads.push_back(
        std::thread(worker_routine, thread_id, &all_threads_bound, &context));
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
}
