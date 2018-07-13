#include "monitor/monitoring_handlers.hpp"

void feedback_handler(
    zmq::socket_t* feedback_puller,
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<std::string, double>& user_latency,
    std::unordered_map<std::string, double>& user_throughput,
    std::unordered_map<Key, std::pair<double, unsigned>>& latency_miss_ratio_map) {
  std::string serialized_feedback = zmq_util::recv_string(feedback_puller);
  UserFeedback fb;
  fb.ParseFromString(serialized_feedback);

  if (fb.has_finish() && fb.finish()) {
    user_latency.erase(fb.uid());
  } else {
    // collect latency and throughput feedback
    user_latency[fb.uid()] = fb.latency();
    user_throughput[fb.uid()] = fb.throughput();

    logger->info("User latency is {}", fb.latency());
    logger->info("User throughput is {}", fb.throughput());

    // collect replication factor adjustment factors
    for (const auto& key_latency_pair : fb.key_latency()) {
      Key key = key_latency_pair.key();
      double observed_key_latency = key_latency_pair.latency();

      if (latency_miss_ratio_map.find(key) == latency_miss_ratio_map.end()) {
        latency_miss_ratio_map[key].first = observed_key_latency / kSloWorst;
        latency_miss_ratio_map[key].second = 1;
      } else {
        latency_miss_ratio_map[key].first =
            (latency_miss_ratio_map[key].first * latency_miss_ratio_map[key].second + observed_key_latency / kSloWorst) /
            (latency_miss_ratio_map[key].second + 1);
        latency_miss_ratio_map[key].second += 1;
      }
    }
  }
}
