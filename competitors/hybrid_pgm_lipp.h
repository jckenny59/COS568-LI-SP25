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
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
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
        // Initially load all data into LIPP for better lookup performance
        return lipp_.Build(data, num_threads);
    }

    size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
        // Update access count for the key
        key_access_count_[lookup_key]++;
        
        // First check LIPP for better lookup performance
        size_t lipp_result = lipp_.EqualityLookup(lookup_key, thread_id);
        if (lipp_result != util::NOT_FOUND) {
            workload_stats_.lookups++;
            return lipp_result;
        }
        
        // If not found in LIPP, check DPGM
        workload_stats_.lookups++;
        size_t dpgm_result = dpgm_.EqualityLookup(lookup_key, thread_id);
        
        // If found in DPGM and it's a hot key, trigger migration
        if (dpgm_result != util::NOT_FOUND && is_hot_key(lookup_key)) {
            trigger_selective_migration(lookup_key);
        }
        
        return dpgm_result;
    }

    uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
        // Combine results from both indexes
        uint64_t lipp_result = lipp_.RangeQuery(lower_key, upper_key, thread_id);
        uint64_t dpgm_result = dpgm_.RangeQuery(lower_key, upper_key, thread_id);
        return lipp_result + dpgm_result;
    }

    void Insert(const KeyType& key, uint32_t thread_id) {
        // First insert into DPGM without lock
        dpgm_.Insert(key, thread_id);
        workload_stats_.inserts++;
        
        // Check migration threshold more frequently
        static size_t insert_count = 0;
        if (++insert_count % 1000 == 0) {  // Check every 1000 inserts
            std::lock_guard<std::mutex> lock(mutex_);
            if (should_flush()) {
                if (!migration_in_progress_.load()) {
                    StartAsyncMigration();
                }
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
        std::atomic<size_t> inserts{0};
        std::atomic<size_t> lookups{0};
        std::atomic<size_t> migrations{0};
    };

    bool should_flush() const {
        // Check if we should flush based on multiple criteria
        auto now = std::chrono::steady_clock::now();
        auto time_since_last_flush = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_flush_time_).count();
        
        // Flush if DPGM size exceeds threshold
        bool size_threshold = dpgm_.size() > migration_threshold_ * (dpgm_.size() + lipp_.size());
        
        // Flush if enough time has passed since last flush
        bool time_threshold = time_since_last_flush > 1000; // 1 second
        
        // Flush if we have enough hot keys to migrate
        bool hot_keys_threshold = hot_keys_.size() > 1000;
        
        return size_threshold || (time_threshold && hot_keys_threshold);
    }

    void adjust_migration_threshold() {
        if (!adaptive_threshold_) return;

        size_t total_ops = workload_stats_.inserts + workload_stats_.lookups;
        if (total_ops == 0) return;

        double insert_ratio = static_cast<double>(workload_stats_.inserts) / total_ops;
        
        // More aggressive threshold adjustment
        if (insert_ratio > 0.7) {
            // Insert-heavy workload: increase threshold more aggressively
            migration_threshold_ = std::min(0.3, migration_threshold_ * 1.2);
        } else if (insert_ratio < 0.3) {
            // Lookup-heavy workload: decrease threshold more aggressively
            migration_threshold_ = std::max(0.01, migration_threshold_ * 0.8);
        }
        
        // Reset stats more frequently
        workload_stats_ = {0, 0, 0};
    }

    void update_hot_keys() {
        // Update hot keys based on access patterns
        std::lock_guard<std::mutex> lock(mutex_);
        hot_keys_.clear();
        
        // Find keys with high access counts
        for (const auto& pair : key_access_count_) {
            if (pair.second > 10) { // Key accessed more than 10 times
                hot_keys_.insert(pair.first);
            }
        }
        
        // Reset access counts periodically
        key_access_count_.clear();
    }

    bool is_hot_key(const KeyType& key) const {
        return hot_keys_.find(key) != hot_keys_.end();
    }

    void trigger_selective_migration(const KeyType& key) {
        if (!migration_in_progress_.load()) {
            std::thread migration_thread([this, key]() {
                MigrateKeyToLIPP(key);
            });
            migration_thread.detach();
        }
    }

    void MigrateKeyToLIPP(const KeyType& key) {
        size_t value = dpgm_.EqualityLookup(key, 0);
        if (value != util::NOT_FOUND) {
            std::lock_guard<std::mutex> lock(mutex_);
            lipp_.Insert(KeyValue<KeyType>{key, value}, 0);
            dpgm_.Delete(key, 0);
        }
    }

    void StartAsyncMigration() {
        bool expected = false;
        if (!migration_in_progress_.compare_exchange_strong(expected, true)) {
            return;
        }
        
        std::thread migration_thread([this]() {
            MigrateDPGMToLIPP();
            migration_in_progress_.store(false);
            workload_stats_.migrations++;
            last_flush_time_ = std::chrono::steady_clock::now();
        });
        migration_thread.detach();
    }

    void MigrateDPGMToLIPP() {
        std::vector<KeyValue<KeyType>> dpgm_data;
        ExtractDPGMData(dpgm_data);
        
        if (dpgm_data.empty()) {
            migration_in_progress_.store(false);
            return;
        }
        
        // Sort data for efficient bulk loading
        std::sort(dpgm_data.begin(), dpgm_data.end(), 
                  [](const KeyValue<KeyType>& a, const KeyValue<KeyType>& b) {
                    return a.key < b.key;
                  });
        
        // Bulk load into LIPP with optimized parameters
        {
            std::lock_guard<std::mutex> lock(mutex_);
            lipp_.Build(dpgm_data, 1);
            dpgm_ = DynamicPGM<KeyType, SearchClass, pgm_error>(std::vector<int>());
        }
    }

    void ExtractDPGMData(std::vector<KeyValue<KeyType>>& output) {
        try {
            output.clear();
            KeyType min_key = std::numeric_limits<KeyType>::min();
            KeyType max_key = std::numeric_limits<KeyType>::max();
            
            // Use larger window size for better performance
            const size_t window_size = 2000000; // Process 2M keys at a time
            KeyType current_min = min_key;
            
            while (current_min <= max_key) {
                KeyType current_max = std::min(current_min + window_size, max_key);
                uint64_t result = dpgm_.RangeQuery(current_min, current_max, 0);
                
                if (result > 0) {
                    for (KeyType key = current_min; key <= current_max; ++key) {
                        size_t value = dpgm_.EqualityLookup(key, 0);
                        if (value != util::NOT_FOUND) {
                            output.push_back(KeyValue<KeyType>{key, value});
                        }
                    }
                }
                
                current_min = current_max + 1;
            }
        } catch (const std::exception& e) {
            std::cerr << "Error in ExtractDPGMData: " << e.what() << std::endl;
        }
    }

    DynamicPGM<KeyType, SearchClass, pgm_error> dpgm_;
    Lipp<KeyType> lipp_;
    double migration_threshold_;
    std::mutex mutex_;
    std::atomic<bool> migration_in_progress_;
    std::atomic<size_t> migration_queue_size_;
    bool adaptive_threshold_;
    WorkloadStats workload_stats_;
    std::thread background_worker_;
    std::atomic<bool> stop_worker_{false};
    
    // New members for improved flushing strategy
    std::unordered_set<KeyType> hot_keys_;
    std::unordered_map<KeyType, size_t> key_access_count_;
    std::chrono::steady_clock::time_point last_flush_time_;
}; 
