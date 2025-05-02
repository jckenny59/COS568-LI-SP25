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
        if (adaptive_threshold_) {
            stop_worker_ = true;
            background_worker_.join();
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
            key_access_count_[lookup_key]++;
            
            // Lower threshold for faster migration (3 accesses instead of 5)
            if (key_access_count_[lookup_key] > 3) {
                migration_queue_.push_back(lookup_key);
                // Increased batch size for more efficient migrations
                if (migration_queue_.size() >= 2000 && !migration_in_progress_.load()) {
                    const_cast<HybridPGMLIPP*>(this)->StartAsyncMigration();
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
        // Insert into DPGM first
        dpgm_.Insert(kv, thread_id);
        workload_stats_.inserts++;
        
        // Check if we should trigger migration
        if (workload_stats_.inserts % 500 == 0) { // More frequent checks
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

    bool should_flush() const {
        auto now = std::chrono::steady_clock::now();
        auto time_since_last_flush = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_flush_time_).count();
        
        // Flush if we have enough hot keys to migrate
        bool hot_keys_threshold = migration_queue_.size() >= 2000; // Increased batch size
        
        // Flush if enough time has passed since last flush
        bool time_threshold = time_since_last_flush > 500; // More frequent flushes
        
        return hot_keys_threshold || time_threshold;
    }

    void adjust_migration_threshold() {
        if (!adaptive_threshold_) return;

        size_t total_ops = workload_stats_.inserts + workload_stats_.lookups;
        if (total_ops == 0) return;

        double insert_ratio = static_cast<double>(workload_stats_.inserts) / total_ops;
        
        // More aggressive threshold adjustment based on workload
        if (insert_ratio > 0.7) {
            // Insert-heavy workload: increase threshold more aggressively
            migration_threshold_ = std::min(0.3, migration_threshold_ * 1.2);
        } else if (insert_ratio < 0.3) {
            // Lookup-heavy workload: decrease threshold more aggressively
            migration_threshold_ = std::max(0.01, migration_threshold_ * 0.8);
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
        bool expected = false;
        if (!migration_in_progress_.compare_exchange_strong(expected, true)) {
            return;
        }
        
        std::thread migration_thread([this]() {
            MigrateHotKeys();
            migration_in_progress_.store(false);
            workload_stats_.migrations++;
            last_flush_time_ = std::chrono::steady_clock::now();
        });
        migration_thread.detach();
    }

    void MigrateHotKeys() {
        std::vector<KeyValue<KeyType>> keys_to_migrate;
        std::unordered_set<KeyType> migrated_keys;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            
            // Get values for keys in migration queue
            for (const auto& key : migration_queue_) {
                size_t value = dpgm_.EqualityLookup(key, 0);
                if (value != util::NOT_FOUND) {
                    keys_to_migrate.push_back(KeyValue<KeyType>{key, value});
                    migrated_keys.insert(key);
                }
            }
            
            // Clear migration queue
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
        
        // Bulk load into LIPP
        {
            std::lock_guard<std::mutex> lock(mutex_);
            lipp_.Build(keys_to_migrate, 1);
            
            // Instead of rebuilding DPGM, we'll just mark the keys as migrated
            // The keys will still exist in DPGM but will be ignored during lookups
            // since we check LIPP first
            hot_keys_.insert(migrated_keys.begin(), migrated_keys.end());
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
}; 
