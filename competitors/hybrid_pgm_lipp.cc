#include "hybrid_pgm_lipp.h"

template<typename Key, typename Payload>
HybridPgmLipp<Key, Payload>::HybridPgmLipp(const Config& cfg)
    : _cfg(cfg), _dpgm(), _lipp() {
    if (cfg.async_flush) {
        _flusher = std::thread([this]() {
            while (!_stop_flag) {
                std::unique_lock<std::mutex> lock(_mtx);
                _cv.wait(lock, [this]() { return _stop_flag || _dpgm_count >= _cfg.flush_threshold; });
                if (_stop_flag) break;
                flush();
            }
        });
    }
}

template<typename Key, typename Payload>
HybridPgmLipp<Key, Payload>::~HybridPgmLipp() {
    if (_cfg.async_flush) {
        _stop_flag = true;
        _cv.notify_one();
        _flusher.join();
    }
}

template<typename Key, typename Payload>
void HybridPgmLipp<Key, Payload>::insert(const Key& k, const Payload& v) {
    std::lock_guard<std::mutex> lock(_mtx);
    _dpgm.insert(k, v);
    ++_dpgm_count;
    maybe_flush();
}

template<typename Key, typename Payload>
bool HybridPgmLipp<Key, Payload>::find(const Key& k, Payload& out) const {
    // First try DPGM
    if (_dpgm.find(k, out)) {
        return true;
    }
    // If not found in DPGM, try LIPP
    return _lipp.find(k, out);
}

template<typename Key, typename Payload>
void HybridPgmLipp<Key, Payload>::flush() {
    // Simple implementation: extract all keys from DPGM and insert into LIPP
    std::vector<std::pair<Key, Payload>> data;
    _dpgm.range_query(_dpgm.begin(), _dpgm.end(), data);
    
    for (const auto& [k, v] : data) {
        _lipp.insert(k, v);
    }
    
    // Clear DPGM after migration
    _dpgm.clear();
    _dpgm_count = 0;
}

template<typename Key, typename Payload>
void HybridPgmLipp<Key, Payload>::maybe_flush() {
    if (_dpgm_count >= _cfg.flush_threshold) {
        if (!_cfg.async_flush) {
            flush();
        } else {
            _cv.notify_one();
        }
    }
}

// Explicit template instantiations
template class HybridPgmLipp<uint64_t, uint64_t>; 