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
        migration_threshold_(100),
        batch_size_(1000),
        hot_key_threshold_(5) {
        // Initialize thread-local storage for hot key tracking
        thread_local_keys_.reserve(1000);
        
        // Parse parameters if provided
        if (!params.empty()) {
            if (params.size() >= 1) migration_threshold_ = params[0];
            if (params.size() >= 2) batch_size_ = params[1];
            if (params.size() >= 3) hot_key_threshold_ = params[2];
        }
    }

    uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
        // Build both indexes in parallel
        std::vector<std::pair<KeyType, uint64_t>> loading_data;
        loading_data.reserve(data.size());
        for (const auto& itm : data) {
            loading_data.emplace_back(itm.key, itm.value);
        }

        uint64_t build_time = util::timing([&] {
            // Build PGM first as it's faster
            dpgm_.Build(data, num_threads);
            
            // Then build LIPP with the same data
            std::vector<KeyValue<KeyType>> lipp_data;
            lipp_data.reserve(data.size());
            for (const auto& [key, value] : loading_data) {
                lipp_data.push_back({key, value});
            }
            lipp_.Build(lipp_data, num_threads);
        });
        
        return build_time;
    }

    size_t EqualityLookup(const KeyType& key, uint64_t& value) const {
        // First check LIPP for hot keys without synchronization
        if (lipp_.EqualityLookup(key, value)) {
            return 1;
        }
        
        // If not in LIPP, check PGM
        if (dpgm_.EqualityLookup(key, value)) {
            // Key found in PGM, consider migrating to LIPP
            const_cast<HybridPGMLIPP*>(this)->consider_migration(key, value);
            return 1;
        }
        
        return 0;
    }

    uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, std::vector<uint64_t>& result) const {
        uint64_t value;
        result.clear();
        
        // Check both indexes and avoid double counting
        std::unordered_set<KeyType> processed_keys;
        
        // First check LIPP
        auto lipp_it = lipp_.lower_bound(lower_key);
        while (lipp_it != lipp_.end() && lipp_it->key() <= upper_key) {
            if (processed_keys.insert(lipp_it->key()).second) {
                result.push_back(lipp_it->value());
            }
            ++lipp_it;
        }
        
        // Then check PGM
        auto pgm_it = dpgm_.lower_bound(lower_key);
        while (pgm_it != dpgm_.end() && pgm_it->key() <= upper_key) {
            if (processed_keys.insert(pgm_it->key()).second) {
                result.push_back(pgm_it->value());
            }
            ++pgm_it;
        }
        
        return result.size();
    }

    void Insert(const KeyType& key, uint64_t value) {
        // Insert into both indexes
        lipp_.Insert(key, value);
        dpgm_.Insert(key, value);
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
        if (count >= migration_threshold_ && consecutive >= hot_key_threshold_) {
            // Try to acquire migration lock
            if (migration_mutex_.try_lock()) {
                try {
                    // Double check if key is still not in LIPP
                    uint64_t dummy;
                    if (!lipp_.EqualityLookup(key, dummy)) {
                        // Migrate key to LIPP
                        lipp_.Insert(key, value);
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
    uint32_t batch_size_;
    uint32_t hot_key_threshold_;

    // Thread-local storage for hot key tracking
    mutable std::vector<std::pair<KeyType, uint64_t>> thread_local_keys_;
    
    // Key statistics
    mutable std::unordered_map<KeyType, KeyStats> key_stats_;
    
    // Synchronization
    mutable std::mutex migration_mutex_;
}; 
