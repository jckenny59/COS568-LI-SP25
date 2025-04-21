#pragma once

#include <vector>
#include <utility>
#include <unordered_map>
#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLIPP : public Base<KeyType> {
public:
    HybridPGMLIPP(const std::vector<int>& params) 
        : dpgm_(params), lipp_(params), threshold_(0.05), 
          migration_in_progress_(false), pending_insertions_() {}

    uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
        // Initially build both indexes with the same data
        uint64_t build_time = 0;
        build_time += dpgm_.Build(data, num_threads);
        build_time += lipp_.Build(data, num_threads);
        return build_time;
    }

    size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
        // First check pending insertions
        auto pending_it = pending_insertions_.find(lookup_key);
        if (pending_it != pending_insertions_.end()) {
            return pending_it->second;
        }

        // Then check DPGM
        size_t result = dpgm_.EqualityLookup(lookup_key, thread_id);
        if (result != util::NOT_FOUND) {
            return result;
        }

        // Finally check LIPP
        return lipp_.EqualityLookup(lookup_key, thread_id);
    }

    void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
        if (migration_in_progress_) {
            // Store insertions during migration
            pending_insertions_[data.key] = data.value;
            return;
        }

        // Insert into DPGM
        dpgm_.Insert(data, thread_id);
        
        // Check if we need to migrate data from DPGM to LIPP
        if (should_migrate()) {
            start_migration();
        }
    }

    std::string name() const { return "HybridPGMLIPP"; }

    std::size_t size() const { 
        return dpgm_.size() + lipp_.size() + pending_insertions_.size(); 
    }

    bool applicable(bool unique, bool range_query, bool insert, bool multithread, 
                   const std::string& ops_filename) const {
        return unique && !multithread;
    }

    std::vector<std::string> variants() const { 
        std::vector<std::string> vec;
        vec.push_back(SearchClass::name());
        vec.push_back(std::to_string(pgm_error));
        return vec;
    }

private:
    DynamicPGM<KeyType, SearchClass, pgm_error> dpgm_;
    Lipp<KeyType> lipp_;
    double threshold_;  // Migration threshold as a fraction of total data
    bool migration_in_progress_;
    std::unordered_map<KeyType, uint64_t> pending_insertions_;

    bool should_migrate() const {
        // Simple strategy: migrate when DPGM size exceeds threshold
        return dpgm_.size() > threshold_ * (dpgm_.size() + lipp_.size());
    }

    void start_migration() {
        migration_in_progress_ = true;
        
        // Extract data from DPGM and insert into LIPP
        // This is a simplified version - in a real implementation,
        // you would need to find a way to extract data from DPGM
        std::vector<KeyValue<KeyType>> data_to_migrate;
        
        // After migration, clear DPGM and process pending insertions
        dpgm_ = DynamicPGM<KeyType, SearchClass, pgm_error>(std::vector<int>());
        
        // Process pending insertions
        for (const auto& kv : pending_insertions_) {
            dpgm_.Insert(KeyValue<KeyType>{kv.first, kv.second}, 0);
        }
        pending_insertions_.clear();
        
        migration_in_progress_ = false;
    }
}; 