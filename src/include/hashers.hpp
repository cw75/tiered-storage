#ifndef __HASHERS_H__
#define __HASHERS_H__

#include "threads.hpp"

struct ThreadHash {
  std::size_t operator()(const ServerThread& st) const {
    return std::hash<std::string>{}(st.get_id());
  }
};

struct GlobalHasher {
  uint32_t operator()(const ServerThread& th) {
    // prepend a string to make the hash value different than
    // what it would be on the naked input
    return std::hash<std::string>{}("GLOBAL" + th.get_virtual_id());
  }

  uint32_t operator()(const Key& key) {
    // prepend a string to make the hash value different than
    // what it would be on the naked input
    return std::hash<std::string>{}("GLOBAL" + key);
  }

  typedef uint32_t ResultType;
};

struct LocalHasher {
  typedef std::hash<std::string>::result_type ResultType;

  ResultType operator()(const ServerThread& th) {
    return std::hash<std::string>{}(std::to_string(th.get_tid()) + "_" +
                                    std::to_string(th.get_virtual_num()));
  }

  ResultType operator()(const Key& key) {
    return std::hash<std::string>{}(key);
  }
};

#endif
