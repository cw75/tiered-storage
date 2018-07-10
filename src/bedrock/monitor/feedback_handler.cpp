#include "monitor/monitoring_handlers.hpp"

void feedback_handler(
    zmq::socket_t* feedback_puller,
    std::unordered_map<std::string, double>& user_latency,
    std::unordered_map<std::string, double>& user_throughput,
    std::unordered_map<Key, std::pair<double, unsigned>>&
        rep_factor_map) {
  std::string serialized_feedback = zmq_util::recv_string(feedback_puller);
  communication::Feedback fb;
  fb.ParseFromString(serialized_feedback);

  if (fb.has_finish() && fb.finish()) {
    user_latency.erase(fb.uid());
  } else {
    // collect latency and throughput feedback
    user_latency[fb.uid()] = fb.latency();
    user_throughput[fb.uid()] = fb.throughput();

    // collect replication factor adjustment factors
    for (const auto& rep : fb.rep()) {
      Key key = rep.key();
      double factor = rep.factor();

      if (rep_factor_map.find(key) == rep_factor_map.end()) {
        rep_factor_map[key].first = factor;
        rep_factor_map[key].second = 1;
      } else {
        rep_factor_map[key].first =
            (rep_factor_map[key].first * rep_factor_map[key].second + factor) /
            (rep_factor_map[key].second + 1);
        rep_factor_map[key].second += 1;
      }
    }
  }
}
