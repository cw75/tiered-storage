#ifndef KEY_VALUE_STORES_ZMQ_UTIL_H_
#define KEY_VALUE_STORES_ZMQ_UTIL_H_

#include <cstring>
#include <string>

#include <zmq.hpp>

// Converts the data within a `zmq::message_t` into a string.
std::string message_to_string(const zmq::message_t& message);

// `send` a string over the socket.
void send_string(const std::string& s, zmq::socket_t* socket);

// `recv` a string over the socket.
std::string recv_string(zmq::socket_t* socket);

// Serialize a proto and `send` it over the socket.
template <typename RequestProto>
void send_proto(const RequestProto& request, zmq::socket_t* socket) {
  std::string request_str;
  request.SerializeToString(&request_str);
  zmq::message_t request_msg(request_str.size());
  memcpy(request_msg.data(), request_str.c_str(), request_str.size());
  socket->send(request_msg);
}

// `recv` a message and unserialize it into a proto.
template <typename ResponseProto>
void recv_proto(ResponseProto* reply, zmq::socket_t* socket) {
  zmq::message_t reply_msg;
  socket->recv(&reply_msg);
  std::string reply_str = message_to_string(reply_msg);
  reply->ParseFromString(reply_str);
}

// `send` a pointer over the socket.
template <typename T>
void send_pointer(const T* const p, zmq::socket_t* socket) {
  zmq::message_t message(sizeof(const T* const));
  memcpy(message.data(), &p, sizeof(const T* const));
  socket->send(message);
}

// `recv` a pointer over the socket.
template <typename T>
T* recv_pointer(zmq::socket_t* socket) {
  zmq::message_t message;
  socket->recv(&message);
  // NOLINT: this code is somewhat forced to be hacky due to the low-level
  // nature of the zeromq API.
  return *reinterpret_cast<T**>(message.data());  // NOLINT
}

#endif  // KEY_VALUE_STORES_ZMQ_UTIL_H_
