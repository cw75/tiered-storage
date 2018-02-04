#ifndef __SERVER_UTILITY_H__
#define __SERVER_UTILITY_H__

#include <string>
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"

using namespace std;

// Define the garbage collect threshold
#define GARBAGE_COLLECT_THRESHOLD 10000000

// Define the gossip period (frequency)
#define PERIOD 10000000

// Define the locatioon of the conf file with the ebs root path
#define EBS_ROOT_FILE "conf/server/ebs_root.txt"

// TODO: reconsider type names here
typedef KV_Store<string, RC_KVS_PairLattice<string>> Database;

// a map that represents which keys should be sent to which IP-port combinations
typedef unordered_map<string, unordered_set<string>> address_keyset_map;

class Serializer {
public:
  virtual RC_KVS_PairLattice<string> get(const string& key, unsigned& err_number) = 0;
  virtual bool put(const string& key, const string& value, const unsigned& timestamp) = 0;
  virtual void remove(const string& key) = 0;
};

class Memory_Serializer : public Serializer {
  Database* kvs_;
public:
  Memory_Serializer(Database* kvs): kvs_(kvs) {}
  RC_KVS_PairLattice<string> get(const string& key, unsigned& err_number) {
    return kvs_->get(key, err_number);
  }
  bool put(const string& key, const string& value, const unsigned& timestamp) {
    timestamp_value_pair<string> p = timestamp_value_pair<string>(timestamp, value);
    return kvs_->put(key, RC_KVS_PairLattice<string>(p));
  }
  void remove(const string& key) {
    kvs_->remove(key);
  }
};

class EBS_Serializer : public Serializer {
  unsigned tid_;
  string ebs_root_;
public:
  EBS_Serializer(unsigned& tid): tid_(tid) {
    ifstream address;

    address.open(EBS_ROOT_FILE);
    std::getline(address, ebs_root_);
    address.close();

    if (ebs_root_.back() != '/') {
      ebs_root_ += "/";
    }
  }
  RC_KVS_PairLattice<string> get(const string& key, unsigned& err_number) {
    RC_KVS_PairLattice<string> res;

    communication::Payload pl;
    string fname = ebs_root_ + "ebs_" + to_string(tid_) + "/" + key;
    // open a new filestream for reading in a binary
    fstream input(fname, ios::in | ios::binary);

    if (!input) {
      err_number = 1;
    } else if (!pl.ParseFromIstream(&input)) {
      cerr << "Failed to parse payload." << endl;
      err_number = 1;
    } else {
      res = RC_KVS_PairLattice<string>(timestamp_value_pair<string>(pl.timestamp(), pl.value()));
    }
    return res;
  }
  bool put(const string& key, const string& value, const unsigned& timestamp) {
    bool replaced = false;
    timestamp_value_pair<string> p = timestamp_value_pair<string>(timestamp, value);

    communication::Payload pl_orig;
    communication::Payload pl;

    string fname = ebs_root_ + "ebs_" + to_string(tid_) + "/" + key;
    fstream input(fname, ios::in | ios::binary);

    if (!input) { // in this case, this key has never been seen before, so we attempt to create a new file for it
      replaced = true;
      pl.set_timestamp(timestamp);
      pl.set_value(value);

      // ios::trunc means that we overwrite the existing file
      fstream output(fname, ios::out | ios::trunc | ios::binary);
      if (!pl.SerializeToOstream(&output)) {
        cerr << "Failed to write payload." << endl;
      }
    } else if (!pl_orig.ParseFromIstream(&input)) { // if we have seen the key before, attempt to parse what was there before
      cerr << "Failed to parse payload." << endl;
    } else {
      // get the existing value that we have and merge
      RC_KVS_PairLattice<string> l = RC_KVS_PairLattice<string>(timestamp_value_pair<string>(pl_orig.timestamp(), pl_orig.value()));
      replaced = l.Merge(p);
      if (replaced) {
        // set the payload's data to the merged values of the value and timestamp
        pl.set_timestamp(l.reveal().timestamp);
        pl.set_value(l.reveal().value);
        // write out the new payload.
        fstream output(fname, ios::out | ios::trunc | ios::binary);
        if (!pl.SerializeToOstream(&output)) {
          cerr << "Failed to write payload\n";
        }
      }
    }
    return replaced;
  }
  void remove(const string& key) {
    string fname = ebs_root_ + "ebs_" + to_string(tid_) + "/" + key;
    if(std::remove(fname.c_str()) != 0) {
      cout << "Error deleting file";
    }
  }
};

// used for key stat monitoring
struct key_stat {
  key_stat() : size_(0), access_(0) {}
  key_stat(unsigned size, unsigned access)
    : size_(size), access_(access) {}
  unsigned size_;
  unsigned access_;
};

// form the timestamp given a time and a thread id
unsigned long long generate_timestamp(unsigned long long time, unsigned tid) {
    unsigned pow = 10;
    while(tid >= pow)
        pow *= 10;
    return time * pow + tid;        
}

void get_replication_factor(
    string& key,
    global_hash_t& global_hash_ring,
    unordered_map<string, key_info>& placement,
    SocketCache& requesters,
    string node_type,
    vector<string>& proxy_address,
    unsigned& seed) {
  string target_address;
  if (node_type == "M") {
    auto threads = responsible_global(key + "_replication", METADATA_MEMORY_REPLICATION_FACTOR, global_hash_ring);
    target_address = next(begin(threads), rand_r(&seed) % threads.size())->get_request_handling_connect_addr();
  } else {
    string target_proxy_address = get_random_proxy_thread(proxy_address, seed).get_key_address_connect_addr();
    auto addresses = get_address_from_other_tier(key + "_replication", requesters[target_proxy_address], node_type, 0, "RH");
    target_address = addresses[rand_r(&seed) % addresses.size()];
  }
  communication::Request req;
  req.set_type("GET");
  prepare_get_tuple(req, key + "_replication");
  auto response = send_request<communication::Request, communication::Response>(req, requesters[target_address]);
  if (response.tuple(0).err_number() == 0) {
    communication::Replication_Factor rep_data;
    rep_data.ParseFromString(response.tuple(0).value());
    placement[key].global_memory_replication_ = rep_data.global_memory_replication();
    placement[key].global_ebs_replication_ = rep_data.global_ebs_replication();
    for (int i = 0; i < rep_data.local_size(); i++) {
      placement[key].local_replication_[rep_data.local(i).ip()] = rep_data.local(i).local_replication();
    }
  } else {
    // TODO: ADD RETRY (hash ring inconsistency issue)
    placement[key] = key_info(DEFAULT_GLOBAL_MEMORY_REPLICATION, DEFAULT_GLOBAL_EBS_REPLICATION);
  }
}

// get all threads responsible for a key from the "node_type" tier
// metadata flag = 0 means the key is a metadata. Otherwise, it is a regular data
unordered_set<server_thread_t, thread_hash> get_responsible_threads(
    string key,
    unsigned metadata_flag,
    global_hash_t& global_hash_ring,
    local_hash_t& local_hash_ring,
    unordered_map<string, key_info>& placement,
    SocketCache& requesters,
    string node_type,
    vector<string>& proxy_address,
    unsigned& seed) {
  if (metadata_flag == 0) {
    if (node_type == "M") {
      return responsible_global(key, METADATA_MEMORY_REPLICATION_FACTOR, global_hash_ring);
    } else {
      // EBS tier doesn't store metadata
      return unordered_set<server_thread_t, thread_hash>();
    }
  } else {
    if (placement.find(key) == placement.end()) {
      get_replication_factor(key, global_hash_ring, placement, requesters, node_type, proxy_address, seed);
    }
    unsigned rep;
    if (node_type == "M") {
      rep = placement[key].global_memory_replication_;
    } else {
      rep = placement[key].global_ebs_replication_;
    }

    unordered_set<server_thread_t, thread_hash> result;

    auto mts = responsible_global(key, rep, global_hash_ring);
    for (auto it = mts.begin(); it != mts.end(); it++) {
      string ip = it->get_ip();
      if (placement[key].local_replication_.find(ip) == placement[key].local_replication_.end()) {
        placement[key].local_replication_[ip] = DEFAULT_LOCAL_REPLICATION;
      }
      auto tids = responsible_local(key, placement[key].local_replication_[ip], local_hash_ring);
      for (auto iter = tids.begin(); iter != tids.end(); iter++) {
        result.insert(server_thread_t(ip, *iter));
      }
    }
    return result;
  }
}

// query proxy for addresses on the other tier and update address map
void query_key_address(communication::Key_Request& key_req, zmq::socket_t& socket, address_keyset_map& addr_keyset_map) {
  auto key_response = send_request<communication::Key_Request, communication::Key_Response>(key_req, socket);
  for (int i = 0; i < key_response.tuple_size(); i++) {
    for (int j = 0; j < key_response.tuple(i).address_size(); j++) {
      addr_keyset_map[key_response.tuple(i).address(j).addr()].insert(key_response.tuple(i).key());
    }
  }
}

#endif