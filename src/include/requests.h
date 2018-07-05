#ifndef __REQUESTS_H__
#define __REQUESTS_H__

template<typename REQ, typename RES>
bool recursive_receive(zmq::socket_t& receiving_socket, zmq::message_t& message, REQ& req, RES& response, bool& succeed) {
  bool rc = receiving_socket.recv(&message);

  if (rc) {
    auto serialized_resp = zmq_util::message_to_string(message);
    response.ParseFromString(serialized_resp);

    if (req.request_id() == response.response_id()) {
      succeed = true;
      return false;
    } else {
      return true;
    }
  } else {
    // timeout
    if (errno == EAGAIN) {
      succeed = false;
    } else {
      succeed = false;
    }

    return false;
  }
}

template<typename REQ, typename RES>
RES send_request(REQ& req, zmq::socket_t& sending_socket, zmq::socket_t& receiving_socket, bool& succeed) {
  string serialized_req;
  req.SerializeToString(&serialized_req);
  zmq_util::send_string(serialized_req, &sending_socket);

  RES response;
  zmq::message_t message;

  bool recurse = recursive_receive<REQ, RES>(receiving_socket, message, req, response, succeed);

  while (recurse) {
    response.Clear();
    zmq::message_t message;
    recurse = recursive_receive<REQ, RES>(receiving_socket, message, req, response, succeed);
  }

  return response;
}

#endif