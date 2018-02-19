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

double get_base(unsigned N, double skew) {
  double base = 0;
  for (unsigned k = 1; k <= N; k++) {
    base += pow(k, -1*skew);
  }
  return base;
}

double get_zipf_prob(unsigned rank, double skew, double base) {
  return pow(rank, -1*skew) / base;
}

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
    zmq::socket_t& key_address_puller,
    string& ip,
    unsigned& thread_id,
    unsigned& rid,
    unsigned& trial) {
  if (trial > 5) {
    logger->info("trial is {} for request for key {}", trial, key);
    cerr << "trial is " + to_string(trial) + " for key " + key + "\n";
    logger->info("Waiting for 5 seconds");
    cerr << "Waiting for 5 seconds\n";
    chrono::seconds dura(5);
    this_thread::sleep_for(dura);
    logger->info("Waited 5s");
    cout << "Waited 5s\n";
  }
  // get worker address
  string worker_address;
  if (key_address_cache.find(key) == key_address_cache.end()) {
    // query the proxy and update the cache
    string target_proxy_address = get_random_proxy_thread(proxy_address, seed).get_key_address_connect_addr();
    bool succeed;
    auto addresses = get_address_from_proxy(ut, key, pushers[target_proxy_address], key_address_puller, succeed, ip, thread_id, rid);
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
    if (key_address_cache[key].size() == 0) {
      cerr << "address cache for key " + key + " has size 0\n";
    }
    worker_address = *(next(begin(key_address_cache[key]), rand_r(&seed) % key_address_cache[key].size()));
  }
  communication::Request req;
  req.set_respond_address(ut.get_request_pulling_connect_addr());
  string req_id = ip + ":" + to_string(thread_id) + "_" + to_string(rid);
  req.set_request_id(req_id);
  rid += 1;
  if (value == "") {
    // get request
    req.set_type("GET");
    communication::Request_Tuple* tp = req.add_tuple();
    tp->set_key(key);
    tp->set_num_address(key_address_cache[key].size());
  } else {
    // put request
    req.set_type("PUT");
    communication::Request_Tuple* tp = req.add_tuple();
    tp->set_key(key);
    tp->set_value(value);
    tp->set_timestamp(0);
    tp->set_num_address(key_address_cache[key].size());
  }
  bool succeed;
  auto res = send_request<communication::Request, communication::Response>(req, pushers[worker_address], response_puller, succeed);
  if (succeed) {
    // initialize the respond string
    if (res.tuple(0).err_number() == 2) {
      // update cache and retry
      //logger->info("cache invalidation due to wrong address");
      key_address_cache.erase(key);
      trial += 1;
      handle_request(key, value, pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
    } else {
      if (res.tuple(0).has_invalidate() && res.tuple(0).invalidate()) {
        //logger->info("cache invalidation of key {} due to address number mismatch", key);
        // update cache
        key_address_cache.erase(key);
      }
    }
  } else {
    logger->info("request timed out when querying worker, clearing cache due to possible node membership change");
    cerr << "request timed out when querying worker, clearing cache due to possible node membership change\n";
    // likely the node has departed. We clear the entries relavant to the worker_address
    vector<string> tokens;
    split(worker_address, ':', tokens);
    string signature = tokens[1];
    unordered_set<string> remove_set;
    for (auto it = key_address_cache.begin(); it != key_address_cache.end(); it++) {
      for (auto iter = it->second.begin(); iter != it->second.end(); iter++) {
        vector<string> v;
        split(*iter, ':', v);
        if (v[1] == signature) {
          remove_set.insert(it->first);
        }
      }
    }
    for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
      key_address_cache.erase(*it);
    }
    trial += 1;
    handle_request(key, value, pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
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

  string monitoring_address;

  address.open("conf/user/monitoring_address.txt");
  getline(address, ip_line);
  monitoring_address = ip_line;
  address.close();

  monitoring_thread_t mt = monitoring_thread_t(monitoring_address);

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  int timeout = 8000;
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

  unsigned rid = 0;

  while (true) {
    zmq_util::poll(-1, &pollitems);

    if (pollitems[0].revents & ZMQ_POLLIN) {
      logger->info("received benchmark command");
      vector<string> v;
      split(zmq_util::recv_string(&command_puller), ':', v);
      string mode = v[0];
      string type = v[1];
      unsigned num_keys = stoi(v[2]);
      unsigned length = stoi(v[3]);
      unsigned report_period = stoi(v[4]);
      unsigned time = stoi(v[5]);
      string contention = v[6];
      // prepare for zipfian workload with coefficient 4 (for high contention)
      unsigned zipf = 4;
      double base = get_base(num_keys, zipf);
      unordered_map<unsigned, double> probability;

      for (unsigned i = 1; i <= num_keys; i++) {
          probability[i] = get_zipf_prob(i, zipf, base);
      }

      // warm up cache
      key_address_cache.clear();
      auto warmup_start = std::chrono::system_clock::now();
      for (unsigned i = 1; i <= num_keys; i++) {
        // key is 8 bytes
        string key = string(8 - to_string(i).length(), '0') + to_string(i);
        if (i % 10000 == 0) {
          logger->info("warming up key {}", key);
        }
        string target_proxy_address = get_random_proxy_thread(proxy_address, seed).get_key_address_connect_addr();
        bool succeed;
        auto addresses = get_address_from_proxy(ut, key, pushers[target_proxy_address], key_address_puller, succeed, ip, thread_id, rid);
        if (succeed) {
          for (auto it = addresses.begin(); it != addresses.end(); it++) {
            key_address_cache[key].insert(*it);
          }
        } else {
          logger->info("timeout during warmup");
        }
      }
      auto warmup_time = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-warmup_start).count();
      logger->info("warming took {} seconds", warmup_time);

      /*if (mode == "MOVEMENT") {
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
              key_aux = to_string(rand_r(&seed) % (num_keys/2) + 1);
            } else {
              key_aux = to_string(rand_r(&seed) % (num_keys/2) + (num_keys/2) + 1);
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
              double throughput = (double)count / (double)time_elapsed;
              logger->info("Throughput is {} ops/seconds", throughput);
              auto latency = (double)1000000 / throughput;
              communication::Latency l;
              l.set_uid(ip + ":" + to_string(thread_id));
              l.set_latency(latency);
              string serialized_latency;
              l.SerializeToString(&serialized_latency);
              zmq_util::send_string(serialized_latency, &pushers[mt.get_latency_report_connect_addr()]);
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
      }*/
      if (mode == "LOAD") {
        size_t count = 0;
        auto benchmark_start = std::chrono::system_clock::now();
        auto benchmark_end = std::chrono::system_clock::now();
        auto epoch_start = std::chrono::system_clock::now();
        auto epoch_end = std::chrono::system_clock::now();
        auto total_time = chrono::duration_cast<std::chrono::seconds>(benchmark_end-benchmark_start).count();

        while (true) {
          string key;
          if (contention == "L") {
            string key_aux = to_string(rand_r(&seed) % (num_keys) + 1);
            key = string(8 - key_aux.length(), '0') + key_aux;
          } else if (contention == "H") {
            double p = rand_r(&seed) / static_cast<double>(RAND_MAX);
            unsigned k = 1;
            while ((p - probability[k]) >= 0) {
                    p -= probability[k];
                    k++;
            }
            key = string(8 - to_string(k).length(), '0') + to_string(k);
          }
          unsigned trial = 1;
          if (type == "G") {
            handle_request(key, "", pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
            count += 1;
          } else if (type == "P") {
            handle_request(key, string(length, 'a'), pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
            count += 1;
          } else if (type == "M") {
            handle_request(key, string(length, 'a'), pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
            trial = 1;
            handle_request(key, "", pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
            count += 2;
          } else {
            logger->info("invalid request type");
          }

          epoch_end = std::chrono::system_clock::now();
          auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(epoch_end-epoch_start).count();
          // report throughput every report_period seconds
          if (time_elapsed >= report_period) {
            double throughput = (double)count / (double)time_elapsed;
            logger->info("Throughput is {} ops/seconds", throughput);
            auto latency = (double)1000000 / throughput;
            communication::Latency l;
            l.set_uid(ip + ":" + to_string(thread_id));
            l.set_latency(latency);
            string serialized_latency;
            l.SerializeToString(&serialized_latency);
            zmq_util::send_string(serialized_latency, &pushers[mt.get_latency_report_connect_addr()]);
            count = 0;
            epoch_start = std::chrono::system_clock::now();
          }

          benchmark_end = std::chrono::system_clock::now();
          total_time = chrono::duration_cast<std::chrono::seconds>(benchmark_end-benchmark_start).count();
          if (total_time > time) {
            break;
          }
          // reset rid
          if (rid > 10000000) {
            rid = 0;
          }
        }
      } else {
        logger->info("invalid experiment mode");
      }
      logger->info("Finished");
      communication::Latency l;
      l.set_uid(ip + ":" + to_string(thread_id));
      l.set_finish(true);
      string serialized_latency;
      l.SerializeToString(&serialized_latency);
      zmq_util::send_string(serialized_latency, &pushers[mt.get_latency_report_connect_addr()]);
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