// Read Uncommited Client.
//
// This file repeatedly
//
//   - reads commands from stdin, as specified in `usage`;
//   - parses the commands into `communication::Request` messages;
//   - sends the requests to the message broker at localhost:5559;
//   - receives a response from the message broker; and
//   - pretty prints the response.
#include <iostream>
#include <string>
#include <vector>

#include <zmq.hpp>

#include "key_value_stores/message.pb.h"
#include "key_value_stores/util.h"

namespace {

std::string usage() {
  return "usage: \n"
         "  BEGIN CHECKPOINT\n"
         "  GET <key>\n"
         "  PUT <key> <value>";
}

}  // namespace

int main() {
  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_REQ);
  socket.connect("tcp://localhost:5559");

  int current_timestamp = -1;
  while (true) {
    std::cout << "Please enter a request: " << std::flush;
    std::string input;
    getline(std::cin, input);
    std::vector<std::string> input_parts;
    split(input, ' ', &input_parts);

    if (input_parts[0] == "BEGIN") {
      communication::Request request;
      request.set_type(communication::Request::BEGIN_TRANSACTION);

      communication::Response response;
      send_request(request, &response, &socket);
      current_timestamp = response.timestamp();
      std::cout << "timestamp is " << current_timestamp << std::endl;
    } else if (input_parts[0] == "GET") {
      communication::Request request;
      request.set_type(communication::Request::GET);
      request.set_key(input_parts[1]);

      communication::Response response;
      send_request(request, &response, &socket);
      std::cout << "timestamp is " << current_timestamp << std::endl;
      std::cout << "value is " << response.value() << std::endl;
    } else if (input_parts[0] == "PUT") {
      communication::Request request;
      request.set_type(communication::Request::PUT);
      request.set_key(input_parts[1]);
      request.set_value(input_parts[2]);
      request.set_timestamp(current_timestamp);

      communication::Response response;
      send_request(request, &response, &socket);
      std::cout << "Successful? " << response.succeed() << std::endl;
    } else {
      std::cout << "Invalid request: " << input << std::endl;
      std::cout << usage() << std::endl;
    }
  }
}
