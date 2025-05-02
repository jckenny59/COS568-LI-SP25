#pragma once

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <mutex>
#include <thread>
#include <atomic>
#include <queue>
#include <limits>
#include <chrono>
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <list>

#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLIPP : public Base<KeyType> {
public:
    HybridPGMLIPP(const std::vector<int>& params) 
        : dpgm_(params), lipp_(params), migration_threshold_(100), 
          batch_size_(1000), hot_key_threshold_(5), migration_interval_(1000),
          migration_in_progress_(false), migration_queue_size_(0),
          adaptive_threshold_(true), workload_stats_({0, 0, 0}),
          hot_keys_(), key_access_count_(), last_flush_time_(std::chrono::steady_clock::now()) {
        
        if (!params.empty()) {
            migration_threshold_ = params[0] / 100.0;
            if (params.size() > 1) {
                adaptive_threshold_ = (params[1] != 0);
            }
        }
        
        // Start background workers
        if (adaptive_threshold_) {
            background_worker_ = std::thread([this]() {
                while (!stop_worker_) {
                    adjust_migration_threshold();
                    update_hot_keys();
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            });
        }
    }

    ~HybridPGMLIPP() {
        cleanup_resources();
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

    size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
        // First check LIPP for hot keys (no synchronization needed)
        uint64_t value;
        if (lipp_.find(lookup_key, value)) {
            // Key is in LIPP, update its access count
            update_hot_keys(lookup_key);
            return value;
        }

        // Key not in LIPP, check PGM
        auto it = dpgm_.find(lookup_key);
        if (it == dpgm_.end()) {
            return util::OVERFLOW;
        }

        // Key found in PGM, update its access count
        update_hot_keys(lookup_key);
        return it->value();
    }

    uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
        pre_operation();
        // For range queries, we need to check both indexes
        uint64_t result = 0;
        uint64_t value;  // Declare value variable here
        
        // First check LIPP
        auto lipp_it = lipp_.lower_bound(lower_key);
        while (lipp_it != lipp_.end() && lipp_it->comp.data.key <= upper_key) {
            result += lipp_it->comp.data.value;
            ++lipp_it;
        }

        // Then check PGM
        auto pgm_it = dpgm_.lower_bound(lower_key);
        while (pgm_it != dpgm_.end() && pgm_it->key() <= upper_key) {
            // Only add if not already counted from LIPP
            if (!lipp_.find(pgm_it->key(), value)) {
                result += pgm_it->value();
            }
            ++pgm_it;
        }

        return result;
    }

    void Insert(const KeyValue<KeyType>& kv, uint32_t thread_id) {
        pre_operation();
        // Insert into both indexes
        dpgm_.Insert(kv, thread_id);
        lipp_.Insert(kv, thread_id);
        workload_stats_.inserts++;
        
        // More frequent migration checks for insert-heavy workloads
        if (workload_stats_.inserts % 50 == 0) { // Reduced from 100
            std::lock_guard<std::mutex> lock(mutex_);
            if (should_flush() && !migration_in_progress_.load()) {
                StartAsyncMigration();
            }
        }
    }

    std::string name() const { return "HybridPGMLIPP"; }

    std::size_t size() const { return dpgm_.size() + lipp_.size(); }

    bool applicable(bool unique, bool range_query, bool insert, bool multithread, 
                   const std::string& ops_filename) const {
        std::string name = SearchClass::name();
        return name != "LinearAVX" && !multithread;
    }

    std::vector<std::string> variants() const { 
        std::vector<std::string> vec;
        vec.push_back(SearchClass::name());
        vec.push_back(std::to_string(pgm_error));
        vec.push_back(std::to_string(migration_threshold_ * 100));
        vec.push_back(adaptive_threshold_ ? "adaptive" : "fixed");
        return vec;
    }

    // Add cleanup before variant switching
    void initSearch() {
        cleanup_resources();
    }

    // Add cleanup after variant switching
    void reset() {
        cleanup_resources();
    }

    // Add cleanup before each operation
    void pre_operation() const {
        // Only check migration if we're not in a critical section
        if (!migration_in_progress_.load(std::memory_order_acquire)) {
            // Use try_lock to avoid blocking
            std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
            if (lock.owns_lock() && should_flush()) {
                const_cast<HybridPGMLIPP*>(this)->StartAsyncMigration();
            }
        }
    }

private:
    struct WorkloadStats {
        mutable std::atomic<size_t> inserts{0};
        mutable std::atomic<size_t> lookups{0};
        mutable std::atomic<size_t> migrations{0};

        void reset() {
            inserts.store(0);
            lookups.store(0);
            migrations.store(0);
        }
    };

    struct KeyStats {
        std::atomic<uint32_t> access_count{0};
        std::atomic<uint32_t> consecutive_accesses{0};
        std::chrono::steady_clock::time_point last_access;
    };

    bool should_flush() const {
        auto now = std::chrono::steady_clock::now();
        auto time_since_last_flush = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_flush_time_).count();
        
        // Smarter workload-aware flushing
        size_t total_ops = workload_stats_.inserts + workload_stats_.lookups;
        if (total_ops == 0) return false;
        
        double insert_ratio = static_cast<double>(workload_stats_.inserts) / total_ops;
        
        // Adjust thresholds based on workload
        size_t min_batch_size = (insert_ratio > 0.7) ? 100 : 200;  // Smaller batches for insert-heavy
        size_t max_wait_time = (insert_ratio > 0.7) ? 50 : 150;    // More frequent flushes for insert-heavy
        
        return migration_queue_.size() >= min_batch_size || time_since_last_flush > max_wait_time;
    }

    void adjust_migration_threshold() {
        if (!adaptive_threshold_) return;

        try {
            size_t total_ops = workload_stats_.inserts + workload_stats_.lookups;
            if (total_ops == 0) return;

            double insert_ratio = static_cast<double>(workload_stats_.inserts) / total_ops;
            
            // More sophisticated threshold adjustment
            if (insert_ratio > 0.7) {
                // Insert-heavy: be more conservative with migrations
                migration_threshold_ = std::min(0.1, migration_threshold_ * 1.02);
            } else if (insert_ratio < 0.3) {
                // Lookup-heavy: be more aggressive with migrations
                migration_threshold_ = std::max(0.005, migration_threshold_ * 0.98);
            } else {
                // Mixed workload: balanced approach
                migration_threshold_ = std::max(0.01, migration_threshold_ * 0.99);
            }
            
            // Clean up old key stats more aggressively
            auto now = std::chrono::steady_clock::now();
            std::vector<KeyType> keys_to_remove;
            
            // First collect keys to remove
            {
                std::lock_guard<std::mutex> lock(mutex_);
                for (const auto& [key, stats] : key_stats_) {
                    auto time_since_last = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        now - stats.last_access).count();
                    if (time_since_last > 250000000) { // 250ms
                        keys_to_remove.push_back(key);
                    }
                }
                
                // Then remove them
                for (const auto& key : keys_to_remove) {
                    key_stats_.erase(key);
                }
            }
            
            workload_stats_.reset();
        } catch (const std::exception& e) {
            std::cerr << "Error adjusting migration threshold: " << e.what() << std::endl;
        }
    }

    void update_hot_keys() {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            
            // Clear old access counts
            key_access_count_.clear();
            
            // Update hot keys set
            hot_keys_.clear();
            for (const auto& key : migration_queue_) {
                hot_keys_.insert(key);
            }
        } catch (const std::exception& e) {
            std::cerr << "Error updating hot keys: " << e.what() << std::endl;
        }
    }

    void StartAsyncMigration() {
        if (migration_in_progress_.load(std::memory_order_acquire)) {
            return;
        }

        try {
            migration_in_progress_.store(true, std::memory_order_release);
            // Store the thread handle to ensure proper cleanup
            migration_thread_ = std::thread([this]() {
                try {
                    MigrateHotKeys();
                } catch (const std::exception& e) {
                    std::cerr << "Migration thread error: " << e.what() << std::endl;
                }
                migration_in_progress_.store(false, std::memory_order_release);
            });
            migration_thread_.detach();
        } catch (const std::exception& e) {
            std::cerr << "Error starting migration thread: " << e.what() << std::endl;
            migration_in_progress_.store(false, std::memory_order_release);
        }
    }

    void MigrateHotKeys() {
        std::vector<std::pair<KeyType, uint64_t>> keys_to_migrate;
        keys_to_migrate.reserve(batch_size_);

        // Collect hot keys
        for (const auto& [key, stats] : key_stats_) {
            if (stats.consecutive_accesses.load(std::memory_order_relaxed) >= hot_key_threshold_) {
                auto it = dpgm_.find(key);
                if (it != dpgm_.end()) {
                    keys_to_migrate.emplace_back(key, it->value());
                }
            }
        }

        // Sort keys for better locality
        std::sort(keys_to_migrate.begin(), keys_to_migrate.end());

        // Bulk load into LIPP
        if (!keys_to_migrate.empty()) {
            lipp_.bulk_load(keys_to_migrate.data(), keys_to_migrate.size());
            
            // Remove migrated keys from PGM
            for (const auto& [key, _] : keys_to_migrate) {
                dpgm_.erase(key);
            }
        }
    }

    void cleanup_resources() {
        try {
            // Stop background worker
            if (adaptive_threshold_) {
                stop_worker_ = true;
                if (background_worker_.joinable()) {
                    background_worker_.join();
                }
            }
            
            // Wait for any ongoing migration to complete
            while (migration_in_progress_.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            
            // Clear all data structures
            {
                std::lock_guard<std::mutex> lock(mutex_);
                migration_queue_.clear();
                hot_keys_.clear();
                key_stats_.clear();
                key_access_count_.clear();
            }

            // Reset state
            migration_in_progress_.store(false, std::memory_order_release);
            stop_worker_ = false;
            workload_stats_.reset();
            last_flush_time_ = std::chrono::steady_clock::now();
        } catch (const std::exception& e) {
            std::cerr << "Error during resource cleanup: " << e.what() << std::endl;
        }
    }

    mutable DynamicPGM<KeyType, SearchClass, pgm_error> dpgm_;
    mutable Lipp<KeyType> lipp_;
    double migration_threshold_;
    double batch_size_;
    uint32_t hot_key_threshold_;
    uint32_t migration_interval_;
    mutable std::atomic<bool> migration_in_progress_;
    mutable std::atomic<size_t> migration_queue_size_;
    bool adaptive_threshold_;
    mutable std::mutex mutex_;
    std::thread background_worker_;
    std::atomic<bool> stop_worker_{false};
    mutable WorkloadStats workload_stats_;
    
    // Improved data structures for tracking hot keys
    mutable std::unordered_map<KeyType, size_t> key_access_count_;
    mutable std::unordered_set<KeyType> hot_keys_;
    mutable std::list<KeyType> migration_queue_;
    mutable std::chrono::steady_clock::time_point last_flush_time_;
    mutable std::unordered_map<KeyType, KeyStats> key_stats_;
    std::thread migration_thread_;
}; 
