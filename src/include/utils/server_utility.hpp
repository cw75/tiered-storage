#ifndef __SERVER_UTILITY_H__
#define __SERVER_UTILITY_H__

#include <string>
#include "communication.pb.h"
#include "../zmq/socket_cache.hpp"
#include "../zmq/zmq_util.hpp"
#include "yaml-cpp/yaml.h"

using namespace std;

// Define the garbage collect threshold
#define GARBAGE_COLLECT_THRESHOLD 10000000

// Define the data redistribute threshold
#define DATA_REDISTRIBUTE_THRESHOLD 50

// Define the gossip period (frequency)
#define PERIOD 10000000

typedef KVStore<string, ReadCommittedPairLattice<string>> MemoryKVS;

// a map that represents which keys should be sent to which IP-port combinations
typedef unordered_map<string, unordered_set<string>> AddressKeysetMap;

class Serializer {
public:
  virtual ReadCommittedPairLattice<string> get(const string& key, unsigned& err_number) = 0;
  virtual bool put(const string& key, const string& value, const unsigned& timestamp) = 0;
  virtual void remove(const string& key) = 0;
};

class MemorySerializer : public Serializer {
  MemoryKVS* kvs_;

public:
  MemorySerializer(MemoryKVS* kvs): kvs_(kvs) {}

  ReadCommittedPairLattice<string> get(const string& key, unsigned& err_number) {
    return kvs_->get(key, err_number);
  }

  bool put(const string& key, const string& value, const unsigned& timestamp) {
    TimestampValuePair<string> p = TimestampValuePair<string>(timestamp, value);
    return kvs_->put(key, ReadCommittedPairLattice<string>(p));
  }

  void remove(const string& key) {
    kvs_->remove(key);
  }
};

class EBSSerializer : public Serializer {
  unsigned tid_;
  string ebs_root_;

public:
  EBSSerializer(unsigned& tid): tid_(tid) {
    YAML::Node conf = YAML::LoadFile("conf/config.yml");

    string monitoring_address = conf["ebs"].as<string>();

    if (ebs_root_.back() != '/') {
      ebs_root_ += "/";
    }
  }

  ReadCommittedPairLattice<string> get(const string& key, unsigned& err_number) {
    ReadCommittedPairLattice<string> res;
    communication::Payload pl;

    // open a new filestream for reading in a binary
    string fname = ebs_root_ + "ebs_" + to_string(tid_) + "/" + key;
    fstream input(fname, ios::in | ios::binary);

    if (!input) {
      err_number = 1;
    } else if (!pl.ParseFromIstream(&input)) {
      cerr << "Failed to parse payload." << endl;
      err_number = 1;
    } else {
      res = ReadCommittedPairLattice<string>(TimestampValuePair<string>(pl.timestamp(), pl.value()));
    }
    return res;
  }

  bool put(const string& key, const string& value, const unsigned& timestamp) {
    bool replaced = false;
    TimestampValuePair<string> p = TimestampValuePair<string>(timestamp, value);

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
      ReadCommittedPairLattice<string> l = ReadCommittedPairLattice<string>(TimestampValuePair<string>(pl_orig.timestamp(), pl_orig.value()));
      replaced = l.merge(p);

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
struct KeyStat {
  KeyStat() : size_(0) {}
  KeyStat(unsigned size)
    : size_(size) {}
  unsigned size_;
};

struct PendingRequest {
  PendingRequest() {}
  PendingRequest(string type, const string& value, string addr, string respond_id)
    : type_(type), value_(value), addr_(addr), respond_id_(respond_id) {}
  string type_;
  string value_;
  string addr_;
  string respond_id_;
};

struct PendingGossip {
  PendingGossip() {}
  PendingGossip(const string& value, const unsigned long long& ts)
    : value_(value), ts_(ts) {}
  string value_;
  unsigned long long ts_;
};

#endif
