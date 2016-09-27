// Item Cut Isolation key-value client.
//
// In Highly Available Transactions: Virtues and Limitations [1], Bailis et al.
// explain that isolation cut isolation transactions can be achieved by having
// clients cache the values of reads, returning the cached values on subsequent
// reads.
//
// [1]: http://www.bailis.org/papers/hat-vldb2014.pdf

#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
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
         "  PUT <key> <value>\n"
         "  END";
}

}  // namespace

int main() {
  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_REQ);
  socket.connect("tcp://localhost:5559");
  std::cout << "client connected to "
            << "tcp://localhost:5559" << std::endl;

  int current_timestamp = -1;

  // As explained at the top of this file, clients buffer their writes, sending
  // them to the server only when the transaction is ended. They also buffer
  // their reads.
  std::unordered_map<std::string, std::string> buffered_writes;
  std::unordered_map<std::string, std::string> buffered_reads;

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
      const std::string &key = input_parts[1];
      if (buffered_writes.count(key) != 0) {
        std::cout << "value is " << buffered_writes[key] << std::endl;
      } else if (buffered_reads.count(key) != 0) {
        std::cout << "value is " << buffered_reads[key] << std::endl;
      } else {
        communication::Request request;
        request.mutable_get()->set_key(key);
        send_proto(request, &socket);

        communication::Response response;
        recv_proto(&response, &socket);
        buffered_reads[key] = response.value();
        std::cout << "value is " << response.value() << std::endl;
      }
    } else if (input_parts[0] == "PUT") {
      const std::string &key = input_parts[1];
      const std::string &value = input_parts[2];
      buffered_writes[key] = value;
    } else if (input_parts[0] == "END") {
      communication::Request request;
      request.mutable_put()->set_timestamp(current_timestamp);
      for (const std::pair<std::string, std::string> &kv : buffered_writes) {
        communication::Request::Put::KeyValuePair *p =
            request.mutable_put()->add_kv_pair();
        p->set_key(std::get<0>(kv));
        p->set_value(std::get<1>(kv));
      }
      send_proto(request, &socket);

      communication::Response response;
      recv_proto(&response, &socket);
      std::cout << "Successful? " << response.succeed() << std::endl;
      buffered_writes.clear();
      buffered_reads.clear();
    } else {
      std::cout << "Invalid request: " << input << std::endl;
      std::cout << usage() << std::endl;
    }
  }
}
