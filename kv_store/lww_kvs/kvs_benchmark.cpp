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
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"
#include "common.h"

using namespace std;

void handle_request(
    string key,
    string value,
    SocketCache& requesters,
    vector<string>& proxy_address,
    unordered_map<string, unordered_set<string>>& key_address_cache,
    unsigned& seed) {
  communication::Request req;
  if (value == "") {
    // get request
    req.set_type("GET");
    prepare_get_tuple(req, key);
  } else {
    // put request
    req.set_type("PUT");
    prepare_put_tuple(req, key, value, 0);
  }
  // get worker address
  string worker_address;
  if (key_address_cache.find(key) == key_address_cache.end()) {
    // query the proxy and update the cache
    string target_proxy_address = get_random_proxy_thread(proxy_address, seed).get_key_address_connect_addr();
    auto addresses = get_address_from_other_tier(key, requesters[target_proxy_address], "U", 1, "RH");
    for (auto it = addresses.begin(); it != addresses.end(); it++) {
      key_address_cache[key].insert(*it);
    }
    worker_address = addresses[rand_r(&seed) % addresses.size()];
  } else {
    worker_address = *(next(begin(key_address_cache[key]), rand_r(&seed) % key_address_cache[key].size()));
  }

  auto res = send_request<communication::Request, communication::Response>(req, requesters[worker_address]);
  // initialize the respond string
  if (res.tuple(0).err_number() == 2) {
    // update cache and retry
    cerr << "cache invalidation\n";
    key_address_cache.erase(key);
    for (int i = 0; i < res.tuple(0).address_size(); i++) {
      key_address_cache[key].insert(res.tuple(0).address(i).addr());
    }
    handle_request(key, value, requesters, proxy_address, key_address_cache, seed);
  }
}

void run(unsigned thread_id) {

  string log_file = "log_" + to_string(thread_id) + ".txt";
  string logger_name = "basic_logger_" + to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  unsigned seed = thread_id;

  // read in the proxy addresses
  vector<string> proxy_address;

  // mapping from key to a set of worker addresses
  unordered_map<string, unordered_set<string>> key_address_cache;

  // read proxy address from the file
  string ip_line;
  ifstream address;
  address.open("conf/user/proxy_address.txt");
  while (getline(address, ip_line)) {
    proxy_address.push_back(ip_line);
  }
  address.close();

  zmq::context_t context(1);
  SocketCache requesters(&context, ZMQ_REQ);

  // responsible for pulling benchmark command
  zmq::socket_t command_puller(context, ZMQ_PULL);
  command_puller.bind("tcp://*:" + to_string(thread_id + COMMAND_BASE_PORT));

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(command_puller), 0, ZMQ_POLLIN, 0 }
  };

  while (true) {
    zmq_util::poll(-1, &pollitems);

    if (pollitems[0].revents & ZMQ_POLLIN) {
      cout << "received benchmark command\n";
      vector<string> v;
      split(zmq_util::recv_string(&command_puller), ':', v);
      string mode = v[0];
      string type = v[1];
      unsigned contention = stoi(v[2]);
      unsigned length = stoi(v[3]);
      unsigned report_period = stoi(v[4]);
      unsigned time = stoi(v[5]);

      // warm up
      for (unsigned i = 1; i <= contention; i++) {
        // key is 8 bytes
        string key = string(8 - to_string(i).length(), '0') + to_string(i);
        if (i % 1000 == 0) {
          logger->info("warming up key {}", key);
          //cout << "warming up key " + key + "\n";
        }
        handle_request(key, string(length, 'a'), requesters, proxy_address, key_address_cache, seed);
      }

      if (mode == "MOVEMENT") {
        int run = 0;

        while (run < 5) {
          size_t count = 0;
          auto benchmark_start = std::chrono::system_clock::now();
          auto benchmark_end = std::chrono::system_clock::now();
          auto epoch_start = std::chrono::system_clock::now();
          auto epoch_end = std::chrono::system_clock::now();
          auto total_time = chrono::duration_cast<std::chrono::seconds>(benchmark_end-benchmark_start).count();

          while (true) {
            string key_aux;
            string key;
            if (run % 2 == 0) {
              key_aux = to_string(rand_r(&seed) % (contention/2) + 1);
            } else {
              key_aux = to_string(rand_r(&seed) % (contention/2) + (contention/2) + 1);
            }
            key = string(8 - key_aux.length(), '0') + key_aux;
            if (type == "G") {
              handle_request(key, "", requesters, proxy_address, key_address_cache, seed);
              count += 1;
            } else if (type == "P") {
              handle_request(key, string(length, 'a'), requesters, proxy_address, key_address_cache, seed);
              count += 1;
            } else if (type == "M") {
              handle_request(key, string(length, 'a'), requesters, proxy_address, key_address_cache, seed);
              handle_request(key, "", requesters, proxy_address, key_address_cache, seed);
              count += 2;
            } else {
              cerr << "invalid request type\n";
            }

            epoch_end = std::chrono::system_clock::now();
            auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(epoch_end-epoch_start).count();
            // report throughput every report_period seconds
            if (time_elapsed >= report_period) {
              logger->info("Throughput is {} ops/seconds", to_string((double)count / (double)time_elapsed));
              //cout << "Throughput is " + to_string((double)count / (double)time_elapsed) + " ops/seconds\n";
              count = 0;
              epoch_start = std::chrono::system_clock::now();
            }

            benchmark_end = std::chrono::system_clock::now();
            total_time = chrono::duration_cast<std::chrono::seconds>(benchmark_end-benchmark_start).count();
            if (total_time > time) {
              break;
            }
          }
          run += 1;
        }
      } else if (mode == "LOAD") {
        size_t count = 0;
        auto benchmark_start = std::chrono::system_clock::now();
        auto benchmark_end = std::chrono::system_clock::now();
        auto epoch_start = std::chrono::system_clock::now();
        auto epoch_end = std::chrono::system_clock::now();
        auto total_time = chrono::duration_cast<std::chrono::seconds>(benchmark_end-benchmark_start).count();

        while (true) {
          string key_aux;
          string key;
          key_aux = to_string(rand_r(&seed) % (contention) + 1);
          key = string(8 - key_aux.length(), '0') + key_aux;
          if (type == "G") {
            handle_request(key, "", requesters, proxy_address, key_address_cache, seed);
            count += 1;
          } else if (type == "P") {
            handle_request(key, string(length, 'a'), requesters, proxy_address, key_address_cache, seed);
            count += 1;
          } else if (type == "M") {
            handle_request(key, string(length, 'a'), requesters, proxy_address, key_address_cache, seed);
            handle_request(key, "", requesters, proxy_address, key_address_cache, seed);
            count += 2;
          } else {
            cerr << "invalid request type\n";
          }

          epoch_end = std::chrono::system_clock::now();
          auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(epoch_end-epoch_start).count();
          // report throughput every report_period seconds
          if (time_elapsed >= report_period) {
            logger->info("Throughput is {} ops/seconds", to_string((double)count / (double)time_elapsed));
            //cout << "Throughput is " + to_string((double)count / (double)time_elapsed) + " ops/seconds\n";
            count = 0;
            epoch_start = std::chrono::system_clock::now();
          }

          benchmark_end = std::chrono::system_clock::now();
          total_time = chrono::duration_cast<std::chrono::seconds>(benchmark_end-benchmark_start).count();
          if (total_time > time) {
            break;
          }
        }
      } else {
        cerr << "invalid experiment mode\n";
      }
      logger->info("Finished");
      cout << "Finished\n";
    }
  }

}

int main(int argc, char* argv[]) {
  if (argc != 1) {
    cerr << "usage:" << argv[0] << endl;
    return 1;
  }

  vector<thread> benchmark_threads;

  for (unsigned thread_id = 1; thread_id < BENCHMARK_THREAD_NUM; thread_id++) {
    benchmark_threads.push_back(thread(run, thread_id));
  }

  run(0);
}