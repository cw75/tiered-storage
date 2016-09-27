#ifndef KEY_VALUE_STORES_BARRIER_H_
#define KEY_VALUE_STORES_BARRIER_H_

#include <condition_variable>
#include <mutex>

// A barrier is a synchronization primitive such that threads enter the barrier
// and block until some specified threshold of threads have entered the
// barrier. Pictorially, consider four threads that work for a while (denoted
// by |) and then enter the barrier and wait (denoted by :). Once all threads
// enter the barrier, they are woken continue working again.
//
//     a    b    c    d
//     |    |    |    | \  working
//     |    |    |    | /
//     |    |    |    : \
//     :    |    |    :  }
//     :    |    |    :  } waiting
//     :    :    |    :  }
//     :    :    |    : /
//     ---------------- barrier
//     |    |    |    |
//     |    |    |    |
//
// Note that once a barrier's threshold has been met, it cannot be reused.
class Barrier {
 public:
  // Construct a barrier that waits for `threshold` threads to enter.
  explicit Barrier(const int threshold) : count_(0), threshold_(threshold) {}
  Barrier(const Barrier& barrier) = delete;
  Barrier& operator=(const Barrier& barrier) = delete;

  // Enter the barrier.
  void enter();

 private:
  int count_;
  int threshold_;
  std::mutex m_;
  std::condition_variable all_done_;
};

#endif  // KEY_VALUE_STORES_BARRIER_H_
