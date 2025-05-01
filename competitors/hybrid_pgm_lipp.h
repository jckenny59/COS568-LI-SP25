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
          adaptive_threshold_(true), workload_stats_({0, 0, 0}) {
        if (!params.empty()) {
            migration_threshold_ = params[0] / 100.0; // Convert percentage to decimal
            if (params.size() > 1) {
                adaptive_threshold_ = (params[1] != 0);
            }
        }
        
        // Start background worker for adaptive threshold adjustment
        if (adaptive_threshold_) {
            background_worker_ = std::thread([this]() {
                while (!stop_worker_) {
                    adjust_migration_threshold();
                    std::this_thread::sleep_for(std::chrono::milliseconds(100)); // More frequent updates
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
        // First check LIPP for better lookup performance
        size_t lipp_result = lipp_.EqualityLookup(lookup_key, thread_id);
        if (lipp_result != util::NOT_FOUND) {
            workload_stats_.lookups++;
            return lipp_result;
        }
        
        // If not found in LIPP, check DPGM
        workload_stats_.lookups++;
        return dpgm_.EqualityLookup(lookup_key, thread_id);
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
        if (++insert_count % 10000 == 0) {  // Check every 10000 inserts
            std::lock_guard<std::mutex> lock(mutex_);
            if (dpgm_.size() > migration_threshold_ * (dpgm_.size() + lipp_.size())) {
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

    void StartAsyncMigration() {
        bool expected = false;
        if (!migration_in_progress_.compare_exchange_strong(expected, true)) {
            return;
        }
        
        std::thread migration_thread([this]() {
            MigrateDPGMToLIPP();
            migration_in_progress_.store(false);
            workload_stats_.migrations++;
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
}; 
