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
        : dpgm_(params), lipp_(params), migration_threshold_(0.05), 
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
        try {
            if (adaptive_threshold_) {
                stop_worker_ = true;
                if (background_worker_.joinable()) {
                    background_worker_.join();
                }
            }
            
            // Wait for any ongoing migration to complete
            while (migration_in_progress_.load()) {
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
        } catch (const std::exception& e) {
            std::cerr << "Error during cleanup: " << e.what() << std::endl;
        }
    }

    uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
        // Initially load all data into DPGM
        uint64_t build_time = dpgm_.Build(data, num_threads);
        
        // Pre-warm LIPP with a smaller sample of keys
        if (data.size() > 0) {
            std::vector<KeyValue<KeyType>> initial_hot_keys;
            size_t sample_size = std::min(data.size(), size_t(100000)); // Reduced to 100K keys
            
            // Sample keys from the middle of the data
            size_t start_idx = data.size() / 2 - sample_size / 2;
            for (size_t i = 0; i < sample_size; ++i) {
                initial_hot_keys.push_back(data[start_idx + i]);
            }
            
            // Sort for efficient bulk loading
            std::sort(initial_hot_keys.begin(), initial_hot_keys.end(),
                     [](const KeyValue<KeyType>& a, const KeyValue<KeyType>& b) {
                         return a.key < b.key;
                     });
            
            // Bulk load into LIPP
            lipp_.Build(initial_hot_keys, 1);
        }
        
        return build_time;
    }

    size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
        // First check LIPP for hot keys
        size_t lipp_result = lipp_.EqualityLookup(lookup_key, thread_id);
        if (lipp_result != util::NOT_FOUND) {
            workload_stats_.lookups++;
            return lipp_result;
        }
        
        // If not found in LIPP, check DPGM
        workload_stats_.lookups++;
        size_t dpgm_result = dpgm_.EqualityLookup(lookup_key, thread_id);
        
        // Update access count and check if key should be migrated
        if (dpgm_result != util::NOT_FOUND) {
            std::lock_guard<std::mutex> lock(mutex_);
            auto& stats = key_stats_[lookup_key];
            auto current_time = std::chrono::steady_clock::now().time_since_epoch().count();
            
            // Check if this is a recent access (within 50ms)
            if (current_time - stats.last_access_time < 50000000) { // 50ms
                stats.consecutive_accesses++;
            } else {
                stats.consecutive_accesses = 1;
            }
            
            // Update statistics
            stats.access_count++;
            stats.total_accesses++;
            stats.last_access_time = current_time;
            
            // Smarter migration decision based on access patterns
            if (!stats.is_hot && 
                (stats.consecutive_accesses >= 2 || // Quick consecutive accesses
                 (stats.total_accesses >= 3 && current_time - stats.last_migration_time > 1000000000))) { // Total access threshold with cooldown
                stats.is_hot = true;
                stats.last_migration_time = current_time;
                
                // Use a temporary queue to avoid deadlocks
                std::vector<KeyType> temp_queue;
                temp_queue.push_back(lookup_key);
                
                // Add to migration queue if not already there
                if (std::find(migration_queue_.begin(), migration_queue_.end(), lookup_key) == migration_queue_.end()) {
                    migration_queue_.push_back(lookup_key);
                }
                
                // Trigger migration if we have enough keys or if this is a very hot key
                if (migration_queue_.size() >= 200 || stats.consecutive_accesses >= 3) {
                    if (!migration_in_progress_.load()) {
                        const_cast<HybridPGMLIPP*>(this)->StartAsyncMigration();
                    }
                }
            }
        }
        
        return dpgm_result;
    }

    uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
        // Combine results from both indexes
        uint64_t lipp_result = lipp_.RangeQuery(lower_key, upper_key, thread_id);
        uint64_t dpgm_result = dpgm_.RangeQuery(lower_key, upper_key, thread_id);
        return lipp_result + dpgm_result;
    }

    void Insert(const KeyValue<KeyType>& kv, uint32_t thread_id) {
        // Optimize insert path by checking if key is already hot
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = key_stats_.find(kv.key);
            if (it != key_stats_.end() && it->second.is_hot) {
                // If key is hot, insert directly into LIPP
                lipp_.Insert(kv, thread_id);
                workload_stats_.inserts++;
                return;
            }
        }
        
        // Otherwise insert into DPGM
        dpgm_.Insert(kv, thread_id);
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
        return unique && !multithread;
    }

    std::vector<std::string> variants() const { 
        std::vector<std::string> vec;
        vec.push_back(SearchClass::name());
        vec.push_back(std::to_string(pgm_error));
        vec.push_back(std::to_string(static_cast<int>(migration_threshold_ * 100)));
        vec.push_back(adaptive_threshold_ ? "adaptive" : "fixed");
        return vec;
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
        size_t access_count{0};
        size_t last_access_time{0};
        bool is_hot{false};
        size_t consecutive_accesses{0};
        size_t total_accesses{0};
        size_t last_migration_time{0};
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
        auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        std::vector<KeyType> keys_to_remove;
        
        // First collect keys to remove
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (const auto& [key, stats] : key_stats_) {
                if (now - stats.last_access_time > 250000000) { // 250ms
                    keys_to_remove.push_back(key);
                }
            }
            
            // Then remove them
            for (const auto& key : keys_to_remove) {
                key_stats_.erase(key);
            }
        }
        
        workload_stats_.reset();
    }

    void update_hot_keys() {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // Clear old access counts
        key_access_count_.clear();
        
        // Update hot keys set
        hot_keys_.clear();
        for (const auto& key : migration_queue_) {
            hot_keys_.insert(key);
        }
    }

    void StartAsyncMigration() {
        if (migration_in_progress_.load()) {
            return;
        }

        try {
            migration_in_progress_.store(true);
            std::thread([this]() {
                try {
                    MigrateHotKeys();
                } catch (const std::exception& e) {
                    std::cerr << "Migration thread error: " << e.what() << std::endl;
                }
                migration_in_progress_.store(false);
            }).detach();
        } catch (const std::exception& e) {
            std::cerr << "Error starting migration thread: " << e.what() << std::endl;
            migration_in_progress_.store(false);
        }
    }

    void MigrateHotKeys() {
        if (migration_queue_.empty()) {
            return;
        }

        try {
            std::vector<KeyValue<KeyType>> keys_to_migrate;
            std::unordered_set<KeyType> migrated_keys;
            
            // Get a snapshot of the migration queue with bounds checking
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (migration_queue_.empty()) {
                    return;
                }
                
                keys_to_migrate.reserve(migration_queue_.size());
                
                for (const auto& key : migration_queue_) {
                    try {
                        size_t value = dpgm_.EqualityLookup(key, 0);
                        if (value != util::NOT_FOUND) {
                            keys_to_migrate.push_back(KeyValue<KeyType>{key, value});
                            migrated_keys.insert(key);
                        }
                    } catch (const std::exception& e) {
                        std::cerr << "Error looking up key during migration: " << e.what() << std::endl;
                        continue;
                    }
                }
                migration_queue_.clear();
            }
            
            if (keys_to_migrate.empty()) {
                return;
            }
            
            // Sort keys for efficient bulk loading
            std::sort(keys_to_migrate.begin(), keys_to_migrate.end(),
                      [](const KeyValue<KeyType>& a, const KeyValue<KeyType>& b) {
                        return a.key < b.key;
                      });
            
            // Bulk load into LIPP with error handling
            {
                std::lock_guard<std::mutex> lock(mutex_);
                try {
                    lipp_.Build(keys_to_migrate, 1);
                    hot_keys_.insert(migrated_keys.begin(), migrated_keys.end());
                } catch (const std::exception& e) {
                    std::cerr << "Error during LIPP bulk load: " << e.what() << std::endl;
                    // Rollback: clear hot keys for failed migration
                    for (const auto& key : migrated_keys) {
                        hot_keys_.erase(key);
                    }
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Critical error during migration: " << e.what() << std::endl;
        }
    }

    mutable DynamicPGM<KeyType, SearchClass, pgm_error> dpgm_;
    mutable Lipp<KeyType> lipp_;
    double migration_threshold_;
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
}; 
