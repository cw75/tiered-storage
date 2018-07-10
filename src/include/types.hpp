#ifndef __TYPES_H__
#define __TYPES_H__

#include <unordered_map>

template <typename T>
using PendingMap = std::unordered_map<std::string, std::vector<T>>;

using AddressType = std::string;

#endif