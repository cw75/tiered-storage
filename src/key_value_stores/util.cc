#include "key_value_stores/util.h"

#include <sstream>
#include <vector>

void split(const std::string& s, char delim, std::vector<std::string>* elems) {
  elems->clear();
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    elems->push_back(item);
  }
}
