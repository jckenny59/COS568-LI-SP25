#include "benchmarks/benchmark_hybrid_pgm_lipp.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

// Benchmark function for hybrid PGM-LIPP index with configurable migration thresholds
template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark, 
                                 bool pareto, const std::vector<int>& params) {
    if (!pareto) {
        // Default configuration with 3% migration threshold for initial testing
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{3});
    } else {
        // Test different migration thresholds for performance analysis
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{2});  // 2% threshold
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{3});  // 3% threshold
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{7});  // 7% threshold
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{15}); // 15% threshold
    }
}

// Benchmark function for different record types and workloads
template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark, 
                                 const std::string& filename) {
    // Handle different datasets and workload types
    if (filename.find("books_100M") != std::string::npos) {
        if (filename.find("0.000000i") != std::string::npos) {
            // Lookup-only workload - prioritize fast lookups
            benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{3});
            benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{3});
        } else if (filename.find("mix") == std::string::npos) {
            if (filename.find("0m") != std::string::npos) {
                // Sequential insert pattern
                benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 16>>(std::vector<int>{3});
                benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 16>>(std::vector<int>{7});
            } else if (filename.find("1m") != std::string::npos) {
                // Random insert pattern
                benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 16>>(std::vector<int>{3});
                benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 16>>(std::vector<int>{7});
            }
        } else {
            // Mixed workloads
            if (filename.find("0.900000i") != std::string::npos) {
                // 90% lookup, 10% insert - optimize for lookups
                benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 16>>(std::vector<int>{3});
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{3});
            } else if (filename.find("0.100000i") != std::string::npos) {
                // 10% lookup, 90% insert - optimize for inserts
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{7});
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{7});
            }
        }
    }
    
    if (filename.find("fb_100M") != std::string::npos) {
        if (filename.find("0.000000i") != std::string::npos) {
            // Facebook dataset lookup-only
            benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{3});
            benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{3});
        } else if (filename.find("mix") == std::string::npos) {
            if (filename.find("0m") != std::string::npos) {
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{3});
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{3});
            }
        } else {
            if (filename.find("0.900000i") != std::string::npos) {
                // Facebook 90% lookup workload
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{3});
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{3});
            } else if (filename.find("0.100000i") != std::string::npos) {
                // Facebook 90% insert workload
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{7});
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{7});
            }
        }
    }
    
    if (filename.find("osmc_100M") != std::string::npos) {
        if (filename.find("0.000000i") != std::string::npos) {
            // Osmc dataset lookup-only
            benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{3});
            benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{3});
        } else if (filename.find("mix") == std::string::npos) {
            if (filename.find("0m") != std::string::npos) {
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{3});
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{3});
            }
        } else {
            if (filename.find("0.900000i") != std::string::npos) {
                // Osmc 90% lookup workload
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{3});
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{3});
            } else if (filename.find("0.100000i") != std::string::npos) {
                // Osmc 90% insert workload
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{7});
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{7});
            }
        }
    }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t); 