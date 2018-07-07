#include "spdlog/spdlog.h"
#include "common.hpp"

using namespace std;

void
feedback_handler(zmq::socket_t *feedback_puller,
                 unordered_map<string, double>& user_latency,
                 unordered_map<string, double>& user_throughput,
                 unordered_map<string, pair<double, unsigned>>& rep_factor_map
                 ) {
  string serialized_feedback = zmq_util::recv_string(feedback_puller);
  communication::Feedback fb;
  fb.ParseFromString(serialized_feedback);

  if (fb.has_finish() && fb.finish()) {
    user_latency.erase(fb.uid());
  } else {
    // collect latency and throughput feedback
    user_latency[fb.uid()] = fb.latency();
    user_throughput[fb.uid()] = fb.throughput();

    // collect replication factor adjustment factors
    for (int i = 0; i < fb.rep_size(); i++) {
      string key = fb.rep(i).key();
      double factor = fb.rep(i).factor();

      if (rep_factor_map.find(key) == rep_factor_map.end()) {
        rep_factor_map[key].first = factor;
        rep_factor_map[key].second = 1;
      } else {
        rep_factor_map[key].first = (rep_factor_map[key].first * rep_factor_map[key].second + factor) / (rep_factor_map[key].second + 1);
        rep_factor_map[key].second += 1;
      }
    }
  }
}