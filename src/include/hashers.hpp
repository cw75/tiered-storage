#ifndef __HASHERS_H__
#define __HASHERS_H__

#include "threads.hpp"

struct ThreadHash {
  std::size_t operator () (const ServerThread &st) const {
    return std::hash<string>{}(st.get_id());
  }
};

struct GlobalHasher {
  uint32_t operator()(const ServerThread& th) {
    // prepend a string to make the hash value different than
    // what it would be on the naked input
    return std::hash<string>{}("GLOBAL"+th.get_virtual_id());
  }

  uint32_t operator()(const string& key) {
    // prepend a string to make the hash value different than
    // what it would be on the naked input
    return std::hash<string>{}("GLOBAL"+key);
  }

  typedef uint32_t result_type;
};

struct LocalHasher {
  hash<string>::result_type operator()(const ServerThread& th) {
    return hash<string>{}(to_string(th.get_tid()) + "_" + to_string(th.get_virtual_num()));
  }

  hash<string>::result_type operator()(const string& key) {
    return hash<string>{}(key);
  }

  typedef hash<string>::result_type result_type;
};

#endif
