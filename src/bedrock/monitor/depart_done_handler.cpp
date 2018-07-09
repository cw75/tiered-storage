#include "common.hpp"
#include "spdlog/spdlog.h"

using namespace std;

void depart_done_handler(
    std::shared_ptr<spdlog::logger> logger, zmq::socket_t* depart_done_puller,
    unordered_map<string, unsigned>& departing_node_map,
    string management_address, bool& removing_memory_node,
    bool& removing_ebs_node,
    chrono::time_point<chrono::system_clock>& grace_start) {
  string msg = zmq_util::recv_string(depart_done_puller);
  vector<string> tokens;
  split(msg, '_', tokens);

  string departed_ip = tokens[0];
  unsigned tier_id = stoi(tokens[1]);

  if (departing_node_map.find(departed_ip) != departing_node_map.end()) {
    departing_node_map[departed_ip] -= 1;

    if (departing_node_map[departed_ip] == 0) {
      if (tier_id == 1) {
        logger->info("Removing memory node {}.", departed_ip);

        string shell_command = "curl -X POST http://" + management_address +
                               "/remove/memory/" + departed_ip;
        system(shell_command.c_str());

        removing_memory_node = false;
      } else {
        logger->info("Removing ebs node {}", departed_ip);

        string shell_command = "curl -X POST http://" + management_address +
                               "/remove/ebs/" + departed_ip;
        system(shell_command.c_str());

        removing_ebs_node = false;
      }

      // reset grace period timer
      grace_start = chrono::system_clock::now();
      departing_node_map.erase(departed_ip);
    }
  } else {
    logger->error("Missing entry in the depart done map.");
  }
}