#include <zmq.hpp>
#include <string>
#include <stdlib.h>
#include <sstream>
#include <fstream>
#include <vector>
#include <iostream>
#include <unistd.h>
#include <memory>
#include <unordered_set>
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"
#include "common.h"

using namespace std;

string handle_request(
    string input,
    SocketCache& requesters,
    vector<string>& proxy_address,
    unordered_map<string, unordered_set<string>>& key_address_cache,
    unsigned& seed) {
  vector<string> v;
  split(input, ' ', v);

  if (v.size() == 0) {
    return "Empty request.\n";
  } else if (v[0] != "GET" && v[0] != "PUT") { 
    return "Invalid request: " + v[0] + ".\n";
  } else {
    string key = v[1];
    communication::Request req;
    req.set_type(v[0]);
    if (v[0] == "GET") {
      prepare_get_tuple(req, key);
    } else {
      prepare_put_tuple(req, key, v[2], 0);
    }
    // get worker address
    string worker_address;
    if (key_address_cache.find(key) == key_address_cache.end()) {
      // query the proxy and update the cache
      string target_proxy_address = get_random_proxy_thread(proxy_address, seed).get_key_address_connect_addr();
      auto addresses = get_address_from_other_tier(key, requesters[target_proxy_address], "U", 1, "RH");
      for (auto it = addresses.begin(); it != addresses.end(); it++) {
        key_address_cache[key].insert(*it);
      }
      worker_address = addresses[rand_r(&seed) % addresses.size()];
    } else {
      worker_address = *(next(begin(key_address_cache[key]), rand_r(&seed) % key_address_cache[key].size()));
    }

    auto res = send_request<communication::Request, communication::Response>(req, requesters[worker_address]);
    // initialize the respond string
    if (v[0] == "GET") {
      if (res.tuple(0).err_number() == 0) {
        return "value of key " + res.tuple(0).key() + " is " + res.tuple(0).value() + "\n";
      } else if (res.tuple(0).err_number() == 1) {
        return "key " + res.tuple(0).key() + " does not exist\n";
      } else {
        // update cache and retry
        cerr << "cache invalidation\n";
        key_address_cache.erase(key);
        for (int i = 0; i < res.tuple(0).address_size(); i++) {
          key_address_cache[key].insert(res.tuple(0).address(i).addr());
        }
        return handle_request(input, requesters, proxy_address, key_address_cache, seed);
      }
    } else {
      if (res.tuple(0).err_number() == 0) {
        return "successfully put key " + res.tuple(0).key() + "\n";
      } else {
        // update cache and retry
        cerr << "cache invalidation\n";
        key_address_cache.erase(key);
        for (int i = 0; i < res.tuple(0).address_size(); i++) {
          key_address_cache[key].insert(res.tuple(0).address(i).addr());
        }
        return handle_request(input, requesters, proxy_address, key_address_cache, seed);
      }
    }
  }
}

int main(int argc, char* argv[]) {

  bool batch;
  if (argc == 1) {
    batch = false;
  } else if (argc == 2) {
    batch = true;
  } else {
    cerr << "invalid argument" << endl;
    return 1;
  }

  // read in the proxy addresses
  vector<string> proxy_address;

  // mapping from key to a set of worker addresses
  unordered_map<string, unordered_set<string>> key_address_cache;

  unsigned seed = time(NULL);

  // read proxy address from the file
  string ip_line;
  ifstream address;
  address.open("conf/user/proxy_address.txt");
  while (getline(address, ip_line)) {
    proxy_address.push_back(ip_line);
  }
  address.close();

  zmq::context_t context(1);
  SocketCache requesters(&context, ZMQ_REQ);

  if (!batch) {
    string input;
    while (true) {
      cout << "kvs> ";
      getline(cin, input);
      cout << handle_request(input, requesters, proxy_address, key_address_cache, seed);
    }
  } else {
    // read in the request
    string request;
    ifstream request_reader;
    request_reader.open(argv[1]);
    while (getline(request_reader, request)) {
      cout << handle_request(request, requesters, proxy_address, key_address_cache, seed);
    }
  }
}
