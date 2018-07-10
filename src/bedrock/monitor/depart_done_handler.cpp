#include "monitor/monitoring_handlers.hpp"

void depart_done_handler(
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* depart_done_puller,
    std::unordered_map<std::string, unsigned>& departing_node_map,
    std::string management_address, bool& removing_memory_node,
    bool& removing_ebs_node,
    std::chrono::time_point<std::chrono::system_clock>& grace_start) {
  std::string msg = zmq_util::recv_string(depart_done_puller);
  std::vector<std::string> tokens;
  split(msg, '_', tokens);

  std::string departed_ip = tokens[0];
  unsigned tier_id = stoi(tokens[1]);

  if (departing_node_map.find(departed_ip) != departing_node_map.end()) {
    departing_node_map[departed_ip] -= 1;

    if (departing_node_map[departed_ip] == 0) {
      if (tier_id == 1) {
        logger->info("Removing memory node {}.", departed_ip);

        std::string shell_command = "curl -X POST http://" +
                                    management_address + "/remove/memory/" +
                                    departed_ip;
        system(shell_command.c_str());

        removing_memory_node = false;
      } else {
        logger->info("Removing ebs node {}", departed_ip);

        std::string shell_command = "curl -X POST http://" +
                                    management_address + "/remove/ebs/" +
                                    departed_ip;
        system(shell_command.c_str());

        removing_ebs_node = false;
      }

      // reset grace period timer
      grace_start = std::chrono::system_clock::now();
      departing_node_map.erase(departed_ip);
    }
  } else {
    logger->error("Missing entry in the depart done map.");
  }
}
