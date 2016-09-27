// READ UNCOMMITTED key-value client.
//
// In Highly Available Transactions: Virtues and Limitations [1], Bailis et al.
// explain that read uncommitted transactions can be achieved by annotating
// each transaction with a globally unique id and enforcing a last-write-wins
// policy on the servers.
//
// [1]: http://www.bailis.org/papers/hat-vldb2014.pdf

#include <iostream>
#include <string>
#include <vector>

#include <zmq.hpp>

#include "key_value_stores/message.pb.h"
#include "key_value_stores/util.h"
#include "key_value_stores/zmq_util.h"

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
  std::cout << "client connected to "
            << "tcp://localhost:5559" << std::endl;

  int current_timestamp = -1;
  while (true) {
    std::cout << "Please enter a request: " << std::flush;
    std::string input;
    getline(std::cin, input);
    std::vector<std::string> input_parts;
    split(input, ' ', &input_parts);

    if (input_parts[0] == "BEGIN") {
      communication::Request request;
      request.mutable_begin_transaction();
      send_proto(request, &socket);

      communication::Response response;
      recv_proto(&response, &socket);
      current_timestamp = response.timestamp();
      std::cout << "timestamp is " << current_timestamp << std::endl;
    } else if (input_parts[0] == "GET") {
      communication::Request request;
      request.mutable_get()->set_key(input_parts[1]);
      send_proto(request, &socket);

      communication::Response response;
      recv_proto(&response, &socket);
      std::cout << "value is " << response.value() << std::endl;
    } else if (input_parts[0] == "PUT") {
      communication::Request request;
      request.mutable_put()->set_timestamp(current_timestamp);
      communication::Request::Put::KeyValuePair *kv_pair =
          request.mutable_put()->add_kv_pair();
      kv_pair->set_key(input_parts[1]);
      kv_pair->set_value(input_parts[2]);
      request.mutable_put()->set_timestamp(current_timestamp);
      send_proto(request, &socket);

      communication::Response response;
      recv_proto(&response, &socket);
      std::cout << "Successful? " << response.succeed() << std::endl;
    } else {
      std::cout << "Invalid request: " << input << std::endl;
      std::cout << usage() << std::endl;
    }
  }
}
