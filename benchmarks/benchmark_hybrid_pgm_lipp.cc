#include "benchmark_hybrid_pgm_lipp.h"

template<typename Key, typename Payload>
void BenchmarkHybridPgmLipp<Key, Payload>::init() {
    typename HybridPgmLipp<Key, Payload>::Config cfg;
    cfg.flush_threshold = 100000; // Example threshold
    cfg.async_flush = false;      // Start with synchronous flush
    _index = std::make_unique<HybridPgmLipp<Key, Payload>>(cfg);
}

template<typename Key, typename Payload>
void BenchmarkHybridPgmLipp<Key, Payload>::insert(const Key& k, const Payload& v) {
    _index->insert(k, v);
}

template<typename Key, typename Payload>
bool BenchmarkHybridPgmLipp<Key, Payload>::find(const Key& k, Payload& out) {
    return _index->find(k, out);
}

// Explicit template instantiations
template class BenchmarkHybridPgmLipp<uint64_t, uint64_t>; 