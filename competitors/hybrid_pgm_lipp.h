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
        : dpgm_(params), lipp_(params), hot_key_threshold_(5) {
        if (!params.empty()) {
            hot_key_threshold_ = params[0];
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
        // First check LIPP for hot keys (fast path)
        size_t lipp_result = lipp_.EqualityLookup(lookup_key, thread_id);
        if (lipp_result != util::NOT_FOUND) {
            update_hot_keys(lookup_key);
            return lipp_result;
        }

        // Key not in LIPP, check PGM (slow path)
        size_t pgm_result = dpgm_.EqualityLookup(lookup_key, thread_id);
        if (pgm_result == util::NOT_FOUND) {
            return util::OVERFLOW;
        }

        // Key found in PGM, update its access count
        update_hot_keys(lookup_key);
        return pgm_result;
    }

    uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
        // Query both indexes
        uint64_t result = lipp_.RangeQuery(lower_key, upper_key, thread_id);
        result += dpgm_.RangeQuery(lower_key, upper_key, thread_id);
        return result;
    }

    void Insert(const KeyValue<KeyType>& kv, uint32_t thread_id) {
        // Always insert into primary index
        lipp_.Insert(kv, thread_id);
        
        // Only insert into cold storage if not hot
        if (hot_keys_.find(kv.key) == hot_keys_.end()) {
            dpgm_.Insert(kv, thread_id);
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
        vec.push_back(std::to_string(hot_key_threshold_));
        return vec;
    }

    void initSearch() {
        cleanup_resources();
    }

    void reset() {
        cleanup_resources();
    }

private:
    void update_hot_keys(const KeyType& key) const {
        static thread_local std::unordered_map<KeyType, uint32_t> local_counts;
        static thread_local uint32_t total_ops = 0;
        
        // Update local counter
        local_counts[key]++;
        total_ops++;
        
        // Periodically sync with global state
        if (total_ops > 1000) {
            std::lock_guard<std::mutex> lock(mutex_);
            for (const auto& [k, count] : local_counts) {
                if (count > hot_key_threshold_) {
                    hot_keys_.insert(k);
                }
            }
            local_counts.clear();
            total_ops = 0;
        }
    }

    void cleanup_resources() {
        std::lock_guard<std::mutex> lock(mutex_);
        hot_keys_.clear();
    }

    mutable DynamicPGM<KeyType, SearchClass, pgm_error> dpgm_;
    mutable Lipp<KeyType> lipp_;
    uint32_t hot_key_threshold_;
    mutable std::mutex mutex_;
    mutable std::unordered_set<KeyType> hot_keys_;
}; 
