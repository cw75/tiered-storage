#ifndef KEY_VALUE_STORES_UTIL_H_
#define KEY_VALUE_STORES_UTIL_H_

#include <cstring>
#include <string>
#include <vector>

#include <zmq.hpp>

// `split(s, c)` splits the string `s` using the delimeter `c` and stores the
// parts in `elems`.
void split(const std::string& s, char delim, std::vector<std::string>* elems);

// Send the request proto `request` over the REQ socket `socket` and receive a
// response proto `response`.
template <typename RequestProto, typename ResponseProto>
void send_request(const RequestProto& request, ResponseProto* response,
                  zmq::socket_t* socket) {
  std::string request_str;
  request.SerializeToString(&request_str);

  zmq::message_t request_msg(request_str.size());
  memcpy(request_msg.data(), request_str.c_str(), request_str.size());
  socket->send(request_msg);

  zmq::message_t reply_msg;
  socket->recv(&reply_msg);

  response->ParseFromString(static_cast<char*>(reply_msg.data()));
}

#endif  // KEY_VALUE_STORES_UTIL_H_
