#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <thread>
#include <atomic>
#include <memory>
#include <shared_mutex>
#include <array>

#include "base.h"
#include "lipp.h"
#include "dynamic_pgm_index.h"
#include "../util.h"

template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLIPP : public Base<KeyType> {
public:
    HybridPGMLIPP(const std::vector<int>& params) : 
        lipp_(params),
        dpgm_(params),
        migration_threshold_(50),  // More aggressive migration
        hot_key_threshold_(3) {    // Lower threshold for hot keys
        // Initialize thread-local storage for hot key tracking
        thread_local_keys_.reserve(1000);
        
        // Parse parameters if provided
        if (!params.empty()) {
            if (params.size() >= 1) migration_threshold_ = params[0];
            if (params.size() >= 2) hot_key_threshold_ = params[1];
        }
    }

    uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
        // Build PGM first as it's faster
        uint64_t build_time = util::timing([&] {
            dpgm_.Build(data, num_threads);
        });
        
        return build_time;
    }

    size_t EqualityLookup(const KeyType& key, uint32_t thread_id) const {
        // First check LIPP for hot keys without synchronization
        size_t lipp_result = lipp_.EqualityLookup(key, thread_id);
        if (lipp_result != util::NOT_FOUND) {
            return lipp_result;
        }
        
        // If not in LIPP, check PGM
        size_t pgm_result = dpgm_.EqualityLookup(key, thread_id);
        if (pgm_result != util::NOT_FOUND) {
            // Key found in PGM, consider migrating to LIPP
            const_cast<HybridPGMLIPP*>(this)->consider_migration(key, pgm_result);
            return pgm_result;
        }
        
        return util::NOT_FOUND;
    }

    uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
        // Query both indexes
        uint64_t result = lipp_.RangeQuery(lower_key, upper_key, thread_id);
        result += dpgm_.RangeQuery(lower_key, upper_key, thread_id);
        return result;
    }

    void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
        // Insert into PGM first
        dpgm_.Insert(data, thread_id);
        
        // Check if key should be in LIPP
        KeyStats& stats = key_stats_[data.key];
        if (stats.access_count.load(std::memory_order_relaxed) >= hot_key_threshold_) {
            lipp_.Insert(data, thread_id);
        }
    }

    bool applicable(bool unique, bool range_query, bool insert, bool multithread, const std::string& ops_filename) const {
        // Exclude LinearAVX as it's not compatible with our hybrid approach
        std::string name = SearchClass::name();
        return name != "LinearAVX" && !multithread;
    }

    std::string name() const { return "HybridPGMLIPP"; }

    std::size_t size() const {
        return lipp_.size() + dpgm_.size();
    }

    bool bulk_insert_supported() const { return true; }

    std::string op_func() const { return "hybrid_pgm_lipp"; }

    std::string search_method() const { return SearchClass::name(); }

    std::string variant() const { return std::to_string(pgm_error); }

    bool multithreaded() const { return false; }

private:
    struct KeyStats {
        std::atomic<uint32_t> access_count{0};
        std::atomic<uint32_t> consecutive_accesses{0};
        std::atomic<bool> is_hot{false};
    };

    void consider_migration(const KeyType& key, uint64_t value) {
        // Get or create key stats
        KeyStats& stats = key_stats_[key];
        
        // Update access count
        uint32_t count = stats.access_count.fetch_add(1, std::memory_order_relaxed) + 1;
        uint32_t consecutive = stats.consecutive_accesses.fetch_add(1, std::memory_order_relaxed) + 1;
        
        // Check if key should be migrated to LIPP
        if (count >= migration_threshold_ || consecutive >= hot_key_threshold_) {
            // Try to acquire migration lock
            if (migration_mutex_.try_lock()) {
                try {
                    // Double check if key is still not in LIPP
                    uint64_t dummy;
                    if (!lipp_.EqualityLookup(key, dummy)) {
                        // Migrate key to LIPP
                        lipp_.Insert(KeyValue<KeyType>{key, value}, 0);
                        stats.is_hot.store(true, std::memory_order_relaxed);
                    }
                } catch (...) {
                    // Handle any exceptions during migration
                }
                migration_mutex_.unlock();
            }
        }
    }

    // Indexes
    Lipp<KeyType> lipp_;
    DynamicPGM<KeyType, SearchClass, pgm_error> dpgm_;

    // Migration parameters
    uint32_t migration_threshold_;
    uint32_t hot_key_threshold_;

    // Thread-local storage for hot key tracking
    mutable std::vector<std::pair<KeyType, uint64_t>> thread_local_keys_;
    
    // Key statistics
    mutable std::unordered_map<KeyType, KeyStats> key_stats_;
    
    // Synchronization
    mutable std::mutex migration_mutex_;
}; 
