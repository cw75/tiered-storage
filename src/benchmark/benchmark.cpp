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
#include "communication.pb.h"
#include "zmq/socket_cache.h"
#include "zmq/zmq_util.h"
#include "common.h"
#include "yaml-cpp/yaml.h"

using namespace std;

double get_base(unsigned N, double skew) {
  double base = 0;
  for (unsigned k = 1; k <= N; k++) {
    base += pow(k, -1*skew);
  }
  return (1/ base);
}

double get_zipf_prob(unsigned rank, double skew, double base) {
  return pow(rank, -1*skew) / base;
}

int sample(int n, unsigned& seed, double base, unordered_map<unsigned, double>& sum_probs) {
  double z;                     // Uniform random number (0 < z < 1)
  int zipf_value;               // Computed exponential value to be returned
  int i;                     // Loop counter
  int low, high, mid;           // Binary-search bounds

  // Pull a uniform random number (0 < z < 1)
  do {
    z = rand_r(&seed) / static_cast<double>(RAND_MAX);
  } while ((z == 0) || (z == 1));

  // Map z to the value
  low = 1, high = n;

  do {
    mid = floor((low+high)/2);
    if (sum_probs[mid] >= z && sum_probs[mid-1] < z) {
      zipf_value = mid;
      break;
    } else if (sum_probs[mid] >= z) {
      high = mid-1;
    } else {
      low = mid+1;
    }
  } while (low <= high);

  // Assert that zipf_value is between 1 and N
  assert((zipf_value >=1) && (zipf_value <= n));

  return zipf_value;
}

void handle_request(
    string key,
    string value,
    SocketCache& pushers,
    vector<string>& routing_address,
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
    logger->info("Trial #{} for request for key {}.", trial, key);
    logger->info("Waiting 5 seconds.");
    chrono::seconds dura(5);
    this_thread::sleep_for(dura);
    logger->info("Waited 5s.");
  }

  // get worker address
  string worker_address;
  if (key_address_cache.find(key) == key_address_cache.end()) {
    // query the routing and update the cache
    string target_routing_address = get_random_routing_thread(routing_address, seed).get_key_address_connect_addr();
    bool succeed;
    auto addresses = get_address_from_routing(ut, key, pushers[target_routing_address], key_address_puller, succeed, ip, thread_id, rid);
    if (succeed) {
      for (auto it = addresses.begin(); it != addresses.end(); it++) {
        key_address_cache[key].insert(*it);
      }
      worker_address = addresses[rand_r(&seed) % addresses.size()];
    } else {
      logger->error("Request timed out when querying routing. This should never happen!");
      return;
    }
  } else {
    if (key_address_cache[key].size() == 0) {
      logger->error("Address cache for key " + key + " has size 0.");
      return;
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
      trial += 1;
      if (trial > 5) {
        for (int i = 0; i < res.tuple(0).addresses_size(); i++) {
          logger->info("Server's return address for key {} is {}.", key, res.tuple(0).addresses(i));
        }
        for (auto it = key_address_cache[key].begin(); it != key_address_cache[key].end(); it++) {
          logger->info("My cached address for key {} is {}", key, *it);
        }
      }

      // update cache and retry
      key_address_cache.erase(key);
      handle_request(key, value, pushers, routing_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
    } else {
      // succeeded
      if (res.tuple(0).has_invalidate() && res.tuple(0).invalidate()) {
        // update cache
        key_address_cache.erase(key);
      }
    }
  } else {
    logger->info("Request timed out when querying worker: clearing cache due to possible node membership changes.");
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
    handle_request(key, value, pushers, routing_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
  }
}

void run(unsigned thread_id) {
  string log_file = "log_" + to_string(thread_id) + ".txt";
  string logger_name = "benchmark_log_" + to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  YAML::Node conf = YAML::LoadFile("conf/config.yml")["user"];
  string ip = conf["ip"].as<string>();

  hash<string> hasher;
  unsigned seed = time(NULL);
  seed += hasher(ip);
  seed += thread_id;
  logger->info("Random seed is {}.", seed);


  // mapping from key to a set of worker addresses
  unordered_map<string, unordered_set<string>> key_address_cache;

  // rep factor map
  unordered_map<string, pair<double, unsigned>> rep_factor_map;

  user_thread_t ut = user_thread_t(ip, thread_id);

  // read the YAML conf

  vector<string> routing_address;
  vector<monitoring_thread_t> mts;

  YAML::Node routing = conf["routing"];
  YAML::Node routing = conf["monitoring"];

  for (YAML::const_iterator it = monitoring.begin(); it != monitoring.end(); ++it) {
    mts.push_back(monitoring_thread_t(it->as<string>()));
  }

  for (YAML::const_iterator it = routing.begin(); it != routing.end(); ++it) {
    routing_address.push_back(it->as<string>());
  }

  int timeout = 10000;
  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);


  // responsible for pulling response
  zmq::socket_t response_puller(context, ZMQ_PULL);
  response_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  response_puller.bind(ut.get_request_pulling_bind_addr());

  // responsible for receiving key address responses
  zmq::socket_t key_address_puller(context, ZMQ_PULL);
  key_address_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  key_address_puller.bind(ut.get_key_address_bind_addr());

  // responsible for pulling benchmark commands
  zmq::socket_t command_puller(context, ZMQ_PULL);
  command_puller.bind("tcp://*:" + to_string(thread_id + COMMAND_BASE_PORT));

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(command_puller), 0, ZMQ_POLLIN, 0 }
  };

  unsigned rid = 0;

  while (true) {
    zmq_util::poll(-1, &pollitems);

    if (pollitems[0].revents & ZMQ_POLLIN) {
      logger->info("Received benchmark command!");
      vector<string> v;

      split(zmq_util::recv_string(&command_puller), ':', v);
      string mode = v[0];

      if (mode == "CACHE") {
        unsigned num_keys = stoi(v[1]);
        // warm up cache
        key_address_cache.clear();
        auto warmup_start = std::chrono::system_clock::now();

        for (unsigned i = 1; i <= num_keys; i++) {
          // key is 8 bytes
          string key = string(8 - to_string(i).length(), '0') + to_string(i);

          if (i % 50000 == 0) {
            logger->info("warming up cache for key {}", key);
          }

          string target_routing_address = get_random_routing_thread(routing_address, seed).get_key_address_connect_addr();
          bool succeed;
          auto addresses = get_address_from_routing(ut, key, pushers[target_routing_address], key_address_puller, succeed, ip, thread_id, rid);

          if (succeed) {
            for (auto it = addresses.begin(); it != addresses.end(); it++) {
              key_address_cache[key].insert(*it);
            }
          } else {
            logger->info("Request timed out during cache warmup.");
          }
        }

        auto warmup_time = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-warmup_start).count();
        logger->info("Cache warm-up took {} seconds.", warmup_time);
      } else if (mode == "LOAD") {
        string type = v[1];
        unsigned num_keys = stoi(v[2]);
        unsigned length = stoi(v[3]);
        unsigned report_period = stoi(v[4]);
        unsigned time = stoi(v[5]);
        double contention = stod(v[6]);

        unordered_map<unsigned, double> sum_probs;
        double base;

        double zipf = contention;

        if (zipf > 0) {
          logger->info("Zipf coefficient is {}.", zipf);
          base = get_base(num_keys, zipf);
          sum_probs[0] = 0;

          for (unsigned i = 1; i <= num_keys; i++) {
            sum_probs[i] = sum_probs[i-1] + base / pow((double) i, zipf);
          }
        } else {
          logger->info("Using a uniform random distribution.");
        }

        size_t count = 0;
        auto benchmark_start = std::chrono::system_clock::now();
        auto benchmark_end = std::chrono::system_clock::now();
        auto epoch_start = std::chrono::system_clock::now();
        auto epoch_end = std::chrono::system_clock::now();
        auto total_time = chrono::duration_cast<std::chrono::seconds>(benchmark_end-benchmark_start).count();
        unsigned epoch = 1;

        while (true) {
          string key;
          unsigned k;

          if (zipf > 0) {
            k = sample(num_keys, seed, base, sum_probs);
          } else {
            k = rand_r(&seed) % (num_keys) + 1;
          }

          key = string(8 - to_string(k).length(), '0') + to_string(k);
          unsigned trial = 1;

          if (type == "G") {
            handle_request(key, "", pushers, routing_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
            count += 1;
          } else if (type == "P") {
            handle_request(key, string(length, 'a'), pushers, routing_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
            count += 1;
          } else if (type == "M") {
            auto req_start = std::chrono::system_clock::now();
            handle_request(key, string(length, 'a'), pushers, routing_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
            trial = 1;

            handle_request(key, "", pushers, routing_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
            count += 2;
            auto req_end = std::chrono::system_clock::now();

            double factor = (double)chrono::duration_cast<std::chrono::microseconds>(req_end-req_start).count() / 2 / SLO_WORST;

            if (rep_factor_map.find(key) == rep_factor_map.end()) {
              rep_factor_map[key].first = factor;
              rep_factor_map[key].second = 1;
            } else {
              rep_factor_map[key].first = (rep_factor_map[key].first * rep_factor_map[key].second + factor) / (rep_factor_map[key].second + 1);
              rep_factor_map[key].second += 1;
            }
          } else {
            logger->info("{} is an invalid request type.", type);
          }

          epoch_end = std::chrono::system_clock::now();
          auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(epoch_end-epoch_start).count();

          // report throughput every report_period seconds
          if (time_elapsed >= report_period) {
            double throughput = (double) count / (double) time_elapsed;
            logger->info("[Epoch {}] Throughput is {} ops/seconds.", epoch, throughput);
            epoch += 1;

            auto latency = (double) 1000000 / throughput;
            communication::Feedback l;

            l.set_uid(ip + ":" + to_string(thread_id));
            l.set_latency(latency);
            l.set_throughput(throughput);

            for (auto it = rep_factor_map.begin(); it != rep_factor_map.end(); it++) {
              //logger->info("factor for key {} is {}", it->first, it->second.first);
              if (it->second.first > 1) {
                communication::Feedback_Rep* r = l.add_rep();
                r->set_key(it->first);
                r->set_factor(it->second.first);
              }
            }

            string serialized_latency;
            l.SerializeToString(&serialized_latency);

            for (int i = 0; i < mts.size(); i++) {
              zmq_util::send_string(serialized_latency, &pushers[mts[i].get_latency_report_connect_addr()]);
            }

            count = 0;
            rep_factor_map.clear();
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

        logger->info("Finished");
        communication::Feedback l;

        l.set_uid(ip + ":" + to_string(thread_id));
        l.set_finish(true);

        string serialized_latency;
        l.SerializeToString(&serialized_latency);

        for (int i = 0; i < mts.size(); i++) {
          zmq_util::send_string(serialized_latency, &pushers[mts[i].get_latency_report_connect_addr()]);
        }
      } else if (mode == "WARM") {
        unsigned num_keys = stoi(v[1]);
        unsigned length = stoi(v[2]);
        unsigned total_threads = stoi(v[3]);
        unsigned range = num_keys / total_threads;
        unsigned start = thread_id * range + 1;
        unsigned end = thread_id * range + 1 + range;

        string key;
        logger->info("Warming up data");
        auto warmup_start = std::chrono::system_clock::now();

        for (unsigned i = start; i < end; i++) {
          unsigned trial = 1;
          key = string(8 - to_string(i).length(), '0') + to_string(i);
          handle_request(key, string(length, 'a'), pushers, routing_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);

          // reset rid
          if (rid > 10000000) {
            rid = 0;
          }

          if (i == (end - start)/2) {
            logger->info("Warmed up half.");
          }
        }

        auto warmup_time = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-warmup_start).count();
        logger->info("Warming up data took {} seconds.", warmup_time);
      } else {
        logger->info("{} is an invalid mode.", mode);
      }
    }
  }

}

int main(int argc, char* argv[]) {
  if (argc != 1) {
    cerr << "Usage: " << argv[0] << endl;
    return 1;
  }

  vector<thread> benchmark_threads;
  for (unsigned thread_id = 1; thread_id < BENCHMARK_THREAD_NUM; thread_id++) {
    benchmark_threads.push_back(thread(run, thread_id));
  }

  run(0);
}
