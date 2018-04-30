#ifndef __SERVER_UTILITY_H__
#define __SERVER_UTILITY_H__

#include <string>
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"

using namespace std;

// Define the garbage collect threshold
#define GARBAGE_COLLECT_THRESHOLD 10000000

// Define the data redistribute threshold
#define DATA_REDISTRIBUTE_THRESHOLD 2

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
  key_stat() : size_(0) {}
  key_stat(unsigned size)
    : size_(size) {}
  unsigned size_;
};

struct pending_request {
  pending_request() {}
  pending_request(string type, const string& value, string addr, string respond_id)
    : type_(type), value_(value), addr_(addr), respond_id_(respond_id) {}
  string type_;
  string value_;
  string addr_;
  string respond_id_;
};

struct pending_gossip {
  pending_gossip() {}
  pending_gossip(const string& value, const unsigned long long& ts)
    : value_(value), ts_(ts) {}
  string value_;
  unsigned long long ts_;
};

// form the timestamp given a time and a thread id
unsigned long long generate_timestamp(unsigned long long time, unsigned tid) {
    unsigned pow = 10;
    while(tid >= pow)
        pow *= 10;
    return time * pow + tid;        
}

#endif