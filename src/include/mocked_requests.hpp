#ifndef SRC_INCLUDE_MOCKED_REQUESTS_HPP_
#define SRC_INCLUDE_MOCKED_REQUESTS_HPP_

template <typename REQ, typename RES>
RES send_request(const REQ& req, zmq::socket_t& sending_socket,
                 zmq::socket_t& receiving_socket, bool& succeed) {
  std::string serialized_req;
  req.SerializeToString(&serialized_req);
  zmq_messaging.send_string(serialized_req, &sending_socket);

  RES response;
  zmq::message_t message;

  bool recurse = recursive_receive<REQ, RES>(receiving_socket, message, req,
                                             response, succeed);

  while (recurse) {
    response.Clear();
    zmq::message_t message;
    recurse = recursive_receive<REQ, RES>(receiving_socket, message, req,
                                          response, succeed);
  }

  return response;
}

template <typename REQ>
void push_request(const REQ& req, zmq::socket_t& socket) {
  std::string serialized_req;
  req.SerializeToString(&serialized_req);
  zmq_messaging.send_string(serialized_req, &socket);
}

#endif // SRC_INCLUDE_MOCKED_REQUESTS_HPP_
