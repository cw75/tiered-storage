#include "key_value_stores/barrier.h"

void Barrier::enter() {
  std::unique_lock<std::mutex> lock(m_);
  count_++;

  // If we're the last thread to enter the barrier, wake up everyone else.
  if (count_ >= threshold_) {
    all_done_.notify_all();
    return;
  }

  // Otherwise, wait for the last thread to arrive and wake us up.
  while (count_ < threshold_) {
    all_done_.wait(lock);
  }
}
