#pragma once

#include "benchmark.h"
#include "hybrid_pgm_lipp.h"

template<typename Key, typename Payload>
class BenchmarkHybridPgmLipp : public Benchmark<Key, Payload> {
public:
  explicit BenchmarkHybridPgmLipp(const std::string& dataset, const std::string& workload)
      : Benchmark<Key, Payload>(dataset, workload) {}

  void init() override {
    typename HybridPgmLipp<Key, Payload>::Config cfg;
    cfg.flush_threshold = 100000; // Example threshold
    cfg.async_flush = false;      // Start with synchronous flush
    _index = std::make_unique<HybridPgmLipp<Key, Payload>>(cfg);
  }

private:
  std::unique_ptr<HybridPgmLipp<Key, Payload>> _index;
}; 