// Copied and modified from http://zguide.zeromq.org/cpp:msgqueue.

//
//  Simple message queuing broker in C++
//  Same as request-reply broker but using QUEUE device
//
// Olivier Chamoux <olivier.chamoux@fr.thalesgroup.com>

#include <iostream>

#include <zmq.hpp>

int main() {
  zmq::context_t context(1);

  //  Socket facing clients
  zmq::socket_t frontend(context, ZMQ_ROUTER);
  frontend.bind("tcp://*:5559");

  //  Socket facing services
  zmq::socket_t backend(context, ZMQ_DEALER);
  backend.bind("tcp://*:5560");

  std::cout << "message broker connecting clients on tcp://*5559 to servers on "
            << "tcp://*:5560" << std::endl;

  //  Start the proxy
  zmq::proxy(frontend, backend, nullptr);
  return 0;
}
