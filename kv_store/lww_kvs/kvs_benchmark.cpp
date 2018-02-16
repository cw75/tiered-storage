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
    SocketCache& pushers,
    vector<string>& proxy_address,
    unordered_map<string, unordered_set<string>>& key_address_cache,
    unsigned& seed,
    shared_ptr<spdlog::logger> logger,
    user_thread_t& ut,
    zmq::socket_t& response_puller,
    zmq::socket_t& key_address_puller) {
  communication::Request req;
  req.set_respond_address(ut.get_request_pulling_connect_addr());
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
    bool succeed;
    auto addresses = get_address_from_proxy(ut, key, pushers[target_proxy_address], key_address_puller, succeed);
    if (succeed) {
      for (auto it = addresses.begin(); it != addresses.end(); it++) {
        key_address_cache[key].insert(*it);
      }
      worker_address = addresses[rand_r(&seed) % addresses.size()];
    } else {
      logger->info("request timed out when querying proxy, this should never happen");
      return;
    }
  } else {
    worker_address = *(next(begin(key_address_cache[key]), rand_r(&seed) % key_address_cache[key].size()));
  }
  bool succeed;
  auto res = send_request<communication::Request, communication::Response>(req, pushers[worker_address], response_puller, succeed);
  if (succeed) {
    // initialize the respond string
    if (res.tuple(0).err_number() == 2) {
      // update cache and retry
      //logger->info("cache invalidation");
      //cerr << "cache invalidation\n";
      key_address_cache.erase(key);
      for (int i = 0; i < res.tuple(0).addresses_size(); i++) {
        key_address_cache[key].insert(res.tuple(0).addresses(i));
      }
      handle_request(key, value, pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller);
    }
  } else {
    logger->info("request timed out when querying worker, clearing cache due to possible node departure");
    // likely the node has departed. For now, we clear the entire cache
    key_address_cache.clear();
    handle_request(key, value, pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller);
  }
}

void run(unsigned thread_id) {

  string log_file = "log_" + to_string(thread_id) + ".txt";
  string logger_name = "basic_logger_" + to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  unsigned seed = time(NULL);
  seed += thread_id;

  string ip = get_ip("user");

  // read in the proxy addresses
  vector<string> proxy_address;

  // mapping from key to a set of worker addresses
  unordered_map<string, unordered_set<string>> key_address_cache;

  user_thread_t ut = user_thread_t(ip, thread_id);

  // read proxy address from the file
  string ip_line;
  ifstream address;
  address.open("conf/user/proxy_address.txt");
  while (getline(address, ip_line)) {
    proxy_address.push_back(ip_line);
  }
  address.close();

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  int timeout = 3000;
  // responsible for pulling response
  zmq::socket_t response_puller(context, ZMQ_PULL);
  response_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  response_puller.bind(ut.get_request_pulling_bind_addr());
  // responsible for receiving depart done notice
  zmq::socket_t key_address_puller(context, ZMQ_PULL);
  key_address_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  key_address_puller.bind(ut.get_key_address_bind_addr());
  // responsible for pulling benchmark command
  zmq::socket_t command_puller(context, ZMQ_PULL);
  command_puller.bind("tcp://*:" + to_string(thread_id + COMMAND_BASE_PORT));

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(command_puller), 0, ZMQ_POLLIN, 0 }
  };

  while (true) {
    zmq_util::poll(-1, &pollitems);

    if (pollitems[0].revents & ZMQ_POLLIN) {
      logger->info("received benchmark command");
      vector<string> v;
      split(zmq_util::recv_string(&command_puller), ':', v);
      string mode = v[0];
      string type = v[1];
      unsigned contention = stoi(v[2]);
      unsigned length = stoi(v[3]);
      unsigned report_period = stoi(v[4]);
      unsigned time = stoi(v[5]);

      // warm up cache
      for (unsigned i = 1; i <= contention; i++) {
        // key is 8 bytes
        string key = string(8 - to_string(i).length(), '0') + to_string(i);
        if (i % 2000 == 0) {
          logger->info("warming up key {}", key);
        }
        communication::Key_Request key_req;
        key_req.set_respond_address(ut.get_key_address_connect_addr());
        key_req.add_keys(key);
        string target_proxy_address = get_random_proxy_thread(proxy_address, seed).get_key_address_connect_addr();
        bool succeed;
        auto key_response = send_request<communication::Key_Request, communication::Key_Response>(key_req, pushers[target_proxy_address], key_address_puller, succeed);
        if (succeed) {
          // update cache
          for (int i = 0; i < key_response.tuple(0).addresses_size(); i++) {
            key_address_cache[key_response.tuple(0).key()].insert(key_response.tuple(0).addresses(i));
          }
        } else {
          logger->info("timeout during warmup");
        }
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
              handle_request(key, "", pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller);
              count += 1;
            } else if (type == "P") {
              handle_request(key, string(length, 'a'), pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller);
              count += 1;
            } else if (type == "M") {
              handle_request(key, string(length, 'a'), pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller);
              handle_request(key, "", pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller);
              count += 2;
            } else {
              logger->info("invalid request type");
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
            handle_request(key, "", pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller);
            count += 1;
          } else if (type == "P") {
            handle_request(key, string(length, 'a'), pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller);
            count += 1;
          } else if (type == "M") {
            handle_request(key, string(length, 'a'), pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller);
            handle_request(key, "", pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller);
            count += 2;
          } else {
            logger->info("invalid request type");
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
        logger->info("invalid experiment mode");
      }
      logger->info("Finished");
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