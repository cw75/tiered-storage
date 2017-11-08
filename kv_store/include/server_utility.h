#ifndef __SERVER_UTILITY_H__
#define __SERVER_UTILITY_H__

#include <string>

using namespace std;

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
template<typename N, typename H>
bool responsible(string key, int rep, consistent_hash_map<N,H>& hash_ring, string id, node_t& sender_node, bool& remove) {
  // the id of the node for which we are checking responsibility
  bool resp = false;

  auto pos = hash_ring.find(key);
  // this is just a dummy value
  auto target_pos = hash_ring.begin();

  // iterate once for every value in the replication factor
  for (int i = 0; i < rep; i++) {
    // if one of the replicas is the node we care about, we set response to
    // true; we also store that node in target_pos
    if (pos->second.id_.compare(id) == 0) {
      resp = true;
      target_pos = pos;
    }

    if (++pos == hash_ring.end()) {
      pos = hash_ring.begin();
    }
  }


  // at the end of the previous loop, pos has the last node responsible for the
  // key; we only remove that key if we have more nodes than there should be replicas
  if (resp && (hash_ring.size() > rep)) {
    remove = true;
    sender_node = pos->second;
  } else if (resp && (hash_ring.size() <= rep)) {
    remove = false;
    if (++target_pos == hash_ring.end()) {
      target_pos = hash_ring.begin();
    }

    sender_node = target_pos->second;
  }

  return resp;
}

#endif