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
#include <condition_variable>

#include "base.h"
#include "lipp.h"
#include "dynamic_pgm_index.h"
#include "../util.h"

template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLIPP : public Base<KeyType> {
public:
    HybridPGMLIPP() {
        // Initialize with more aggressive thresholds
        migration_threshold_ = 50;  // Lower threshold to migrate more keys
        batch_size_ = 500;         // Smaller batches for more frequent migrations
        hot_key_threshold_ = 3;    // Lower threshold to detect hot keys faster
        migration_in_progress_ = false;
        migration_thread_ = nullptr;
        migration_thread_running_ = false;
        migration_thread_ready_ = false;
        migration_thread_mutex_ = std::make_unique<std::mutex>();
        migration_thread_cv_ = std::make_unique<std::condition_variable>();
        migration_thread_ready_cv_ = std::make_unique<std::condition_variable>();
    }

    HybridPGMLIPP(const std::vector<int>& params) : 
        lipp_(params),
        dpgm_(params) {
        // Initialize with more aggressive thresholds
        migration_threshold_ = 50;  // Lower threshold to migrate more keys
        batch_size_ = 500;         // Smaller batches for more frequent migrations
        hot_key_threshold_ = 3;    // Lower threshold to detect hot keys faster
        migration_in_progress_ = false;
        migration_thread_ = nullptr;
        migration_thread_running_ = false;
        migration_thread_ready_ = false;
        migration_thread_mutex_ = std::make_unique<std::mutex>();
        migration_thread_cv_ = std::make_unique<std::condition_variable>();
        migration_thread_ready_cv_ = std::make_unique<std::condition_variable>();

        // Parse parameters if provided
        if (!params.empty()) {
            if (params.size() >= 1) migration_threshold_ = params[0];
            if (params.size() >= 2) hot_key_threshold_ = params[1];
            if (params.size() >= 3) batch_size_ = params[2];
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
        // First try LIPP for hot keys
        if (lipp_.EqualityLookup(data.key, data.value)) {
            return;  // Key already in LIPP
        }

        // Check if key is hot
        auto& stats = key_stats_[data.key];
        uint64_t current_count = stats.access_count.fetch_add(1, std::memory_order_relaxed);
        uint64_t current_consecutive = stats.consecutive_accesses.fetch_add(1, std::memory_order_relaxed);
        
        // More aggressive hot key detection
        bool is_hot = (current_count >= hot_key_threshold_) || 
                     (current_consecutive >= 2) ||  // Consider keys accessed twice in a row as hot
                     (current_count >= 2 && current_consecutive >= 1);  // Consider keys with 2+ total accesses and 1+ consecutive as hot

        if (is_hot) {
            // Try to insert into LIPP first
            lipp_.Insert(data, thread_id);
            return;
        }

        // Fall back to PGM
        dpgm_.Insert(data, thread_id);

        // Check if we should trigger migration
        if (!migration_in_progress_.load(std::memory_order_relaxed) && 
            key_stats_.size() >= migration_threshold_) {
            TriggerMigration();
        }
    }

    void Insert(const KeyType& key, uint64_t value) {
        // First try LIPP for hot keys
        if (lipp_.EqualityLookup(key, value)) {
            return;  // Key already in LIPP
        }

        // Check if key is hot
        auto& stats = key_stats_[key];
        uint64_t current_count = stats.access_count.fetch_add(1, std::memory_order_relaxed);
        uint64_t current_consecutive = stats.consecutive_accesses.fetch_add(1, std::memory_order_relaxed);
        
        // More aggressive hot key detection
        bool is_hot = (current_count >= hot_key_threshold_) || 
                     (current_consecutive >= 2) ||  // Consider keys accessed twice in a row as hot
                     (current_count >= 2 && current_consecutive >= 1);  // Consider keys with 2+ total accesses and 1+ consecutive as hot

        if (is_hot) {
            // Try to insert into LIPP first
            lipp_.Insert(KeyValue<KeyType>{key, value}, 0);
            return;
        }

        // Fall back to PGM
        dpgm_.Insert(KeyValue<KeyType>{key, value}, 0);

        // Check if we should trigger migration
        if (!migration_in_progress_.load(std::memory_order_relaxed) && 
            key_stats_.size() >= migration_threshold_) {
            TriggerMigration();
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

    void TriggerMigration() {
        if (migration_in_progress_.exchange(true, std::memory_order_acquire)) {
            return;
        }

        // Start migration thread if not running
        if (!migration_thread_running_.load(std::memory_order_relaxed)) {
            migration_thread_ = std::make_unique<std::thread>(&HybridPGMLIPP::MigrationThread, this);
            migration_thread_running_.store(true, std::memory_order_release);
            
            // Wait for thread to be ready
            std::unique_lock<std::mutex> lock(*migration_thread_mutex_);
            migration_thread_ready_cv_->wait(lock, [this] { 
                return migration_thread_ready_.load(std::memory_order_acquire); 
            });
        }

        // Notify migration thread
        {
            std::lock_guard<std::mutex> lock(*migration_thread_mutex_);
            migration_thread_ready_.store(false, std::memory_order_release);
        }
        migration_thread_cv_->notify_one();
    }

    void MigrationThread() {
        while (true) {
            // Wait for migration signal
            {
                std::unique_lock<std::mutex> lock(*migration_thread_mutex_);
                migration_thread_ready_.store(true, std::memory_order_release);
                migration_thread_ready_cv_->notify_one();
                migration_thread_cv_->wait(lock, [this] { 
                    return !migration_thread_ready_.load(std::memory_order_acquire); 
                });
            }

            try {
                // Collect hot keys more aggressively
                std::vector<std::pair<KeyType, uint64_t>> hot_keys;
                hot_keys.reserve(batch_size_);
                
                // First pass: collect definitely hot keys
                for (const auto& [key, stats] : key_stats_) {
                    if (stats.access_count.load(std::memory_order_relaxed) >= hot_key_threshold_ ||
                        stats.consecutive_accesses.load(std::memory_order_relaxed) >= 2) {
                        uint64_t value;
                        if (dpgm_.EqualityLookup(key, value) != util::NOT_FOUND) {
                            hot_keys.emplace_back(key, value);
                        }
                    }
                }

                // Second pass: collect potentially hot keys if we have space
                if (hot_keys.size() < batch_size_) {
                    for (const auto& [key, stats] : key_stats_) {
                        if (hot_keys.size() >= batch_size_) break;
                        
                        if (stats.access_count.load(std::memory_order_relaxed) >= 2) {
                            uint64_t value;
                            if (dpgm_.EqualityLookup(key, value) != util::NOT_FOUND) {
                                hot_keys.emplace_back(key, value);
                            }
                        }
                    }
                }

                // Migrate hot keys to LIPP
                if (!hot_keys.empty()) {
                    // Convert to KeyValue format
                    std::vector<KeyValue<KeyType>> lipp_data;
                    lipp_data.reserve(hot_keys.size());
                    for (const auto& [key, value] : hot_keys) {
                        lipp_data.push_back({key, value});
                    }
                    
                    // Build LIPP with hot keys
                    lipp_.Build(lipp_data, 1);
                    
                    // Remove migrated keys from PGM
                    for (const auto& [key, _] : hot_keys) {
                        dpgm_.remove(key);
                    }
                }

                // Clear stats for migrated keys
                for (const auto& [key, _] : hot_keys) {
                    key_stats_.erase(key);
                }

            } catch (const std::exception& e) {
                // Log error and continue
                std::cerr << "Migration error: " << e.what() << std::endl;
            }

            migration_in_progress_.store(false, std::memory_order_release);
        }
    }

    // Indexes
    Lipp<KeyType> lipp_;
    DynamicPGM<KeyType, SearchClass, pgm_error> dpgm_;

    // Migration parameters
    uint32_t migration_threshold_;
    uint32_t hot_key_threshold_;
    uint32_t batch_size_;
    std::atomic<bool> migration_in_progress_;
    std::unique_ptr<std::thread> migration_thread_;
    std::atomic<bool> migration_thread_running_;
    std::atomic<bool> migration_thread_ready_;
    std::unique_ptr<std::mutex> migration_thread_mutex_;
    std::unique_ptr<std::condition_variable> migration_thread_cv_;
    std::unique_ptr<std::condition_variable> migration_thread_ready_cv_;
    
    // Key statistics
    mutable std::unordered_map<KeyType, KeyStats> key_stats_;
    
    // Synchronization
    mutable std::mutex migration_mutex_;
}; 
