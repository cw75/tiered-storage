#ifndef __COMMON_H__
#define __COMMON_H__

#include <string>
#include <boost/functional/hash.hpp>
#include <boost/format.hpp>
#include <boost/crc.hpp>

// Define the number of threads
#define THREAD_NUM 2

// Define replication factor
#define REPLICATION 2

using namespace std;

struct node_t {
    node_t() {}
    node_t(string ip, size_t port): ip_(ip), port_(port) {
        id_ = ip + ":" + to_string(port);
        client_connection_addr_ = "tcp://" + ip + ":" + to_string(port - 1000);
        dgossip_addr_ = "tcp://" + id_;
        lgossip_addr_ = "inproc://" + to_string(port);
    }
    string id_;
    string ip_;
    size_t port_;
    string client_connection_addr_;
    string dgossip_addr_;
    string lgossip_addr_;
};

bool operator<(const node_t& l, const node_t& r) {
    if (l.id_.compare(r.id_) == 0) return false;
    else return true;  
}

struct crc32_hasher {
    uint32_t operator()(const node_t& node) {
        boost::crc_32_type ret;
        ret.process_bytes(node.id_.c_str(), node.id_.size());
        return ret.checksum();
    }
    uint32_t operator()(const string& key) {
        boost::crc_32_type ret;
        ret.process_bytes(key.c_str(), key.size());
        return ret.checksum();
    }
    typedef uint32_t result_type;
};

#endif