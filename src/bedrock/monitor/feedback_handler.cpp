#include "monitor/monitoring_handlers.hpp"

void feedback_handler(
    zmq::socket_t* feedback_puller,
    std::unordered_map<std::string, double>& user_latency,
    std::unordered_map<std::string, double>& user_throughput,
    std::unordered_map<Key, std::pair<double, unsigned>>& bump_factor_map) {
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
    for (const auto& key_latency_pair : fb.key_latency()) {
      Key key = key_latency_pair.key();
      double observed_key_latency = key_latency_pair.latency();

      if (bump_factor_map.find(key) == bump_factor_map.end()) {
        bump_factor_map[key].first = observed_key_latency / kSloWorst;
        bump_factor_map[key].second = 1;
      } else {
        bump_factor_map[key].first =
            (bump_factor_map[key].first * bump_factor_map[key].second + observed_key_latency / kSloWorst) /
            (bump_factor_map[key].second + 1);
        bump_factor_map[key].second += 1;
      }
    }
  }
}
