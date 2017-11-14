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

#endif