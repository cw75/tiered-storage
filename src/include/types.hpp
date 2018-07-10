#ifndef __TYPES_H__
#define __TYPES_H__

#include <unordered_map>

template <typename T>
using PendingMap = std::unordered_map<std::string, std::vector<T>>;

using Address = std::string;

using Key = std::string;

using StorageStat =
    std::unordered_map<Address,
                       std::unordered_map<unsigned, unsigned long long>>;

using OccupancyStat = std::unordered_map<
    Address, std::unordered_map<unsigned, std::pair<double, unsigned>>>;

using AccessStat =
    std::unordered_map<Address, std::unordered_map<unsigned, unsigned>>;

#endif