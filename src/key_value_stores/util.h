#ifndef KEY_VALUE_STORES_UTIL_H_
#define KEY_VALUE_STORES_UTIL_H_

#include <string>
#include <vector>

// `split(s, c)` splits the string `s` using the delimeter `c` and stores the
// parts in `elems`.
void split(const std::string& s, char delim, std::vector<std::string>* elems);

#endif  // KEY_VALUE_STORES_UTIL_H_
