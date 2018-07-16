#include <stdio.h>
#include <stdlib.h>

#include <iostream>

#include "test_server_handlers.hpp"

MockZmqMessaging mock_zmq_messaging;
ZmqMessagingInterface* kZmqMessagingInterface = &mock_zmq_messaging;

MockResponsibleThread mock_responsible_thread;
ResponsibleThreadInterface* kResponsibleThreadInterface =
    &mock_responsible_thread;

unsigned kDefaultLocalReplication = 1;
unsigned kSelfTierId = 1;
unsigned kThreadNum = 4;
std::vector<unsigned> kSelfTierIdVector = {kSelfTierId};

int main(int argc, char* argv[]) {
  logger->set_level(spdlog::level::off);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
