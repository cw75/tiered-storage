#ifndef __SERVER_UTILITY_H__
#define __SERVER_UTILITY_H__

#include <fstream>
#include <string>

#include "../kvs/base_kv_store.hpp"
#include "../kvs/rc_pair_lattice.hpp"
#include "../zmq/socket_cache.hpp"
#include "../zmq/zmq_util.hpp"
#include "communication.pb.h"
#include "yaml-cpp/yaml.h"

// Define the garbage collect threshold
#define GARBAGE_COLLECT_THRESHOLD 10000000

// Define the data redistribute threshold
#define DATA_REDISTRIBUTE_THRESHOLD 50

// Define the gossip period (frequency)
#define PERIOD 10000000

typedef KVStore<Key, ReadCommittedPairLattice<std::string>> MemoryKVS;

// a map that represents which keys should be sent to which IP-port combinations
typedef std::unordered_map<Address, std::unordered_set<Key>> AddressKeysetMap;

class Serializer {
 public:
  virtual ReadCommittedPairLattice<std::string> get(const Key& key,
                                                    unsigned& err_number) = 0;
  virtual bool put(const Key& key, const std::string& value,
                   const unsigned& timestamp) = 0;
  virtual void remove(const Key& key) = 0;
};

class MemorySerializer : public Serializer {
  MemoryKVS* kvs_;

 public:
  MemorySerializer(MemoryKVS* kvs) : kvs_(kvs) {}

  ReadCommittedPairLattice<std::string> get(const Key& key,
                                            unsigned& err_number) {
    return kvs_->get(key, err_number);
  }

  bool put(const Key& key, const std::string& value,
           const unsigned& timestamp) {
    TimestampValuePair<std::string> p =
        TimestampValuePair<std::string>(timestamp, value);
    return kvs_->put(key, ReadCommittedPairLattice<std::string>(p));
  }

  void remove(const Key& key) { kvs_->remove(key); }
};

class EBSSerializer : public Serializer {
  unsigned tid_;
  std::string ebs_root_;

 public:
  EBSSerializer(unsigned& tid) : tid_(tid) {
    YAML::Node conf = YAML::LoadFile("conf/config.yml");

    ebs_root_ = conf["ebs"].as<std::string>();

    if (ebs_root_.back() != '/') {
      ebs_root_ += "/";
    }
  }

  ReadCommittedPairLattice<std::string> get(const Key& key,
                                            unsigned& err_number) {
    ReadCommittedPairLattice<std::string> res;
    communication::Payload pl;

    // open a new filestream for reading in a binary
    std::string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;
    std::fstream input(fname, std::ios::in | std::ios::binary);

    if (!input) {
      err_number = 1;
    } else if (!pl.ParseFromIstream(&input)) {
      std::cerr << "Failed to parse payload." << std::endl;
      err_number = 1;
    } else {
      res = ReadCommittedPairLattice<std::string>(
          TimestampValuePair<std::string>(pl.timestamp(), pl.value()));
    }
    return res;
  }

  bool put(const Key& key, const std::string& value,
           const unsigned& timestamp) {
    bool replaced = false;
    TimestampValuePair<std::string> p =
        TimestampValuePair<std::string>(timestamp, value);

    communication::Payload pl_orig;
    communication::Payload pl;

    std::string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;
    std::fstream input(fname, std::ios::in | std::ios::binary);

    if (!input) {  // in this case, this key has never been seen before, so we
                   // attempt to create a new file for it
      replaced = true;
      pl.set_timestamp(timestamp);
      pl.set_value(value);

      // ios::trunc means that we overwrite the existing file
      std::fstream output(fname,
                          std::ios::out | std::ios::trunc | std::ios::binary);
      if (!pl.SerializeToOstream(&output)) {
        std::cerr << "Failed to write payload." << std::endl;
      }
    } else if (!pl_orig.ParseFromIstream(
                   &input)) {  // if we have seen the key before, attempt to
                               // parse what was there before
      std::cerr << "Failed to parse payload." << std::endl;
    } else {
      // get the existing value that we have and merge
      ReadCommittedPairLattice<std::string> l =
          ReadCommittedPairLattice<std::string>(TimestampValuePair<std::string>(
              pl_orig.timestamp(), pl_orig.value()));
      replaced = l.merge(p);

      if (replaced) {
        // set the payload's data to the merged values of the value and
        // timestamp
        pl.set_timestamp(l.reveal().timestamp);
        pl.set_value(l.reveal().value);

        // write out the new payload.
        std::fstream output(fname,
                            std::ios::out | std::ios::trunc | std::ios::binary);

        if (!pl.SerializeToOstream(&output)) {
          std::cerr << "Failed to write payload" << std::endl;
        }
      }
    }

    return replaced;
  }

  void remove(const Key& key) {
    std::string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;

    if (std::remove(fname.c_str()) != 0) {
      std::cerr << "Error deleting file" << std::endl;
    }
  }
};

struct PendingRequest {
  PendingRequest() {}
  PendingRequest(std::string type, const std::string& value, Address addr,
                 std::string respond_id) :
      type_(type),
      value_(value),
      addr_(addr),
      respond_id_(respond_id) {}

  // TODO(vikram): change these type names
  std::string type_;
  std::string value_;
  Address addr_;
  std::string respond_id_;
};

struct PendingGossip {
  PendingGossip() {}
  PendingGossip(const std::string& value, const unsigned long long& ts) :
      value_(value),
      ts_(ts) {}
  std::string value_;
  unsigned long long ts_;
};

#endif
