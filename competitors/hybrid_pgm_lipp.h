#pragma once

#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include "dynamic_pgm_index.h"
#include "lipp_index.h"

template<typename Key, typename Payload>
class HybridPgmLipp {
public:
  struct Config {
    size_t flush_threshold;       // e.g. 5% of expected total keys
    bool   async_flush = false;   // use background thread?
    // You can also embed sub-configs if needed:
    // typename DynamicPgmIndex<Key,Payload>::Config pgm_cfg;
    // typename LippIndex<Key,Payload>::Config   lipp_cfg;
  };

  explicit HybridPgmLipp(const Config& cfg);
  ~HybridPgmLipp();

  void insert(const Key& k, const Payload& v);
  bool find(const Key& k, Payload& out) const;

private:
  void flush();       // migrate from DPGM → LIPP
  void maybe_flush(); // check threshold and trigger flush

  DynamicPgmIndex<Key,Payload> _dpgm;
  LippIndex<Key,Payload>       _lipp;
  Config                       _cfg;
  size_t                       _dpgm_count = 0;

  // for async flush:
  std::thread                 _flusher;
  std::atomic<bool>           _stop_flag{false};
  mutable std::mutex          _mtx;
  std::condition_variable     _cv;
};

// Include the template implementations
#include "hybrid_pgm_lipp.cc"
