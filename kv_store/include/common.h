#ifndef __COMMON_H__
#define __COMMON_H__

#include <string>
#include <boost/functional/hash.hpp>
#include <boost/format.hpp>
#include <boost/crc.hpp>
#include <functional>
#include <curl/curl.h>

using namespace std;

// Define global replication factor
#define REPLICATION 2

struct node_t {
    node_t() {}
    node_t(string ip, size_t port): ip_(ip), port_(port) {
        id_ = ip + ":" + to_string(port);
        client_connection_addr_ = "tcp://" + ip + ":" + to_string(port - 100);
        dgossip_addr_ = "tcp://" + id_;
        lgossip_addr_ = "inproc://" + to_string(port);
        lredistribute_addr_ = "inproc://" + to_string(port + 100);
        lthread_depart_addr_ = "inproc://" + to_string(port + 200);
        ldepart_done_addr_ = "inproc://" + to_string(port + 300);
        node_join_addr_ = "tcp://" + ip + ":" + to_string(port + 100);
        node_depart_addr_ = "tcp://" + ip + ":" + to_string(port + 200);
        key_exchange_addr_ = "tcp://" + ip + ":" + to_string(port + 300);
    }
    string id_;
    string ip_;
    size_t port_;
    string client_connection_addr_;
    string dgossip_addr_;
    string lgossip_addr_;
    string lredistribute_addr_;
    string node_join_addr_;
    string node_depart_addr_;
    string key_exchange_addr_;
    string lthread_depart_addr_;
    string ldepart_done_addr_;
};

bool operator<(const node_t& l, const node_t& r) {
    if (l.id_.compare(r.id_) == 0) return false;
    else return true;
}

bool operator==(const node_t& l, const node_t& r) {
    if (l.id_.compare(r.id_) == 0) return true;
    else return false;
}

struct node_hash {
    std::size_t operator () (const node_t &n) const {
        return std::hash<string>{}(n.id_);
    }
};

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

struct ebs_hasher {
    hash<string>::result_type operator()(const node_t& node) {
        return hash<string>{}(node.id_);
    }
    hash<string>::result_type operator()(const string& key) {
        return hash<string>{}(key);
    }
    typedef hash<string>::result_type result_type;
};

void split(const string &s, char delim, vector<string> &elems) {
    stringstream ss(s);
    string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
}

size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

string getIP() {
    CURL *curl;
    CURLcode res;
    string readBuffer;

    curl = curl_easy_init();
    if(curl) {
    curl_easy_setopt(curl, CURLOPT_URL, "http://169.254.169.254/latest/meta-data/public-ipv4");
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
    res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);
    }
    return readBuffer;
}



#endif
