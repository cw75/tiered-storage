#ifndef __HASHERS_H__
#define __HASHERS_H__

#include "threads.h"

struct thread_hash {
  std::size_t operator () (const server_thread_t &st) const {
    return std::hash<string>{}(st.get_id());
  }
};

struct global_hasher {
  uint32_t operator()(const server_thread_t& th) {
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

struct local_hasher {
  hash<string>::result_type operator()(const server_thread_t& th) {
    return hash<string>{}(to_string(th.get_tid()) + "_" + to_string(th.get_virtual_num()));
  }

  hash<string>::result_type operator()(const string& key) {
    return hash<string>{}(key);
  }

  typedef hash<string>::result_type result_type;
};

#endif
