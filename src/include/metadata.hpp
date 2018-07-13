//  Copyright 2018 U.C. Berkeley RISE Lab
// 
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
// 
//      http://www.apache.org/licenses/LICENSE-2.0
// 
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#ifndef SRC_INCLUDE_METADATA_HPP_
#define SRC_INCLUDE_METADATA_HPP_

// represents the replication state for each key
struct KeyInfo {
  std::unordered_map<unsigned, unsigned> global_replication_map_;
  std::unordered_map<unsigned, unsigned> local_replication_map_;
};

// per-tier metadata
struct TierData {
  TierData() : thread_number_(1), default_replication_(1), node_capacity_(0) {}

  TierData(unsigned t_num, unsigned rep, unsigned long long node_capacity) :
      thread_number_(t_num),
      default_replication_(rep),
      node_capacity_(node_capacity) {}

  unsigned thread_number_;

  unsigned default_replication_;

  unsigned long long node_capacity_;
};

inline bool is_metadata(Key key) {
  std::vector<std::string> v;
  split(key, '_', v);

  if (v[0] == "BEDROCKMETADATA") {
    return true;
  } else {
    return false;
  }
}

// NOTE: This needs to be here because it needs the definition of TierData
extern std::unordered_map<unsigned, TierData> kTierDataMap;

#endif  // SRC_INCLUDE_METADATA_HPP_
