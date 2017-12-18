#ifndef __SERVER_UTILITY_H__
#define __SERVER_UTILITY_H__

#include <string>

using namespace std;

// Define the default local replication factor
#define DEFAULT_LOCAL_REPLICATION 1

string alphabet("abcdefghijklmnopqrstuvwxyz");

string getNextDeviceID(string currentID) {
	char first = currentID.at(0);
	char second = currentID.at(1);
	if (second != 'z')
		return string(1, first) + string(1, alphabet.at(alphabet.find(second) + 1));
	else {
		if (first == 'b')
			return "ca";
		else
			return "error: name out of bound\n";
	}
}

struct storage_key_info {
  storage_key_info() : global_memory_replication_(1), global_ebs_replication_(2), local_replication_(1) {}
  storage_key_info(int gmr, int ger, int lr)
    : global_memory_replication_(gmr), global_ebs_replication_(ger), local_replication_(lr) {}
  storage_key_info(int gmr, int ger)
    : global_memory_replication_(gmr), global_ebs_replication_(ger), local_replication_(DEFAULT_LOCAL_REPLICATION) {}
  int global_memory_replication_;
  int global_ebs_replication_;
  int local_replication_;
};

struct pair_hash {
  template <class T1, class T2>
    std::size_t operator () (const std::pair<T1,T2> &p) const {
      auto h1 = std::hash<T1>{}(p.first);
      auto h2 = std::hash<T2>{}(p.second);

      return h1 ^ h2;
    }
};

// we have this template because we use responsible for with different hash
// functions depending on whether we are checking local or remote
// responsibility
// New comment by chenggang: simplified the code
// assuming the replication factor will never be greater than the number of nodes in a tier
template<typename N, typename H>
pair<bool, N*> responsible(string key, int rep, consistent_hash_map<N,H>& hash_ring, string id) {
  // the id of the node for which we are checking responsibility
  bool resp = false;

  auto pos = hash_ring.find(key);

  // iterate once for every value in the replication factor
  for (int i = 0; i < rep; i++) {
    // if one of the replicas is the node we care about, we set response to
    // true; we also store that node in target_pos
    if (pos->second.id_.compare(id) == 0) {
      resp = true;
    }

    if (++pos == hash_ring.end()) {
      pos = hash_ring.begin();
    }
  }


  // at the end of the previous loop, pos has the last node responsible for the key
  if (resp) {
    return pair<bool, N*>(resp, &(pos->second));
  } else {
    return pair<bool, N*>(resp, nullptr);
  }
}

// contact target_node_address to get the worker address that's responsible for the keys
template<typename T>
communication::Key_Response get_key_address(string target_node_address, string source_tier, unordered_set<string> keys, SocketCache& requesters, unordered_map<string, T> placement) {
  // form key address request
  communication::Key_Request req;
  req.set_sender("server");
  if (source_tier == "M") {
    req.set_source_tier("M");
  } else if (source_tier == "E") {
    req.set_source_tier("E");
  }
  for (auto it = keys.begin(); it != keys.end(); it++) {
    communication::Key_Request_Tuple* tp = req.add_tuple();
    tp->set_key(*it);
    tp->set_global_memory_replication(placement[*it].global_memory_replication_);
    tp->set_global_ebs_replication(placement[*it].global_ebs_replication_);
  }
  string key_req;
  req.SerializeToString(&key_req);
  // send key address request
  zmq_util::send_string(key_req, &requesters[target_node_address]);
  // receive key address response
  string key_res = zmq_util::recv_string(&requesters[target_node_address]);

  communication::Key_Response resp;
  resp.ParseFromString(key_res);
  return resp;
}

#endif