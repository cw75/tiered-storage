#include "communication.pb.h"
#include "common.h"
#include "threads.h"
#include "requests.h"

using namespace std;

// node capacity in KB
unsigned MEM_NODE_CAPACITY = 60000000;
unsigned EBS_NODE_CAPACITY = 256000000;


void split(const std::string &s, char delim, std::vector<std::string> &elems) {
  std::stringstream ss(s);
  std::string item;

  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
}

// form the timestamp given a time and a thread id
unsigned long long generate_timestamp(unsigned long long time, unsigned tid) {
  unsigned pow = 10;
  while(tid >= pow)
    pow *= 10;
  return time * pow + tid;
}

void prepare_get_tuple(communication::Request& req, string key) {
  communication::Request_Tuple* tp = req.add_tuple();
  tp->set_key(key);
}

void prepare_put_tuple(communication::Request& req, string key, string value, unsigned long long timestamp) {
  communication::Request_Tuple* tp = req.add_tuple();
  tp->set_key(key);
  tp->set_value(value);
  tp->set_timestamp(timestamp);
}

void push_request(communication::Request& req, zmq::socket_t& socket) {
  string serialized_req;
  req.SerializeToString(&serialized_req);
  zmq_util::send_string(serialized_req, &socket);
}

bool is_metadata(string key) {
  vector<string> v;
  split(key, '_', v);

  if (v[0] == "BEDROCKMETADATA") {
    return true;
  } else {
    return false;
  }
}
