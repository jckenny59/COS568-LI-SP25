#include "benchmarks/benchmark_hybrid_pgm_lipp.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

namespace hybrid_benchmark {

template <typename Searcher>
void run_hybrid_benchmark(tli::Benchmark<uint64_t>& benchmark, 
                         bool pareto_mode, 
                         const std::vector<int>& parameters) {
    if (!pareto_mode) {
        // Default configuration with 5% migration threshold and adaptive mode
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{5, 1});
    } else {
        // Test different migration thresholds and modes
        // Format: {threshold_percentage, adaptive_mode}
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{1, 1});   // 1% threshold, adaptive
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{5, 1});   // 5% threshold, adaptive
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{10, 1});  // 10% threshold, adaptive
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{20, 1});  // 20% threshold, adaptive
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{5, 0});   // 5% threshold, fixed
    }
}

template <int record>
void run_hybrid_workload_benchmark(tli::Benchmark<uint64_t>& benchmark, 
                                  const std::string& workload_file) {
    
    if (workload_file.find("books_100M") != std::string::npos) {
        if (workload_file.find("0.000000i") != std::string::npos) {
            // Lookup-only workload - prioritize fast lookups
            benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{2, 1});
            benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{2, 1});
        } else if (workload_file.find("mix") == std::string::npos) {
            if (workload_file.find("0m") != std::string::npos) {
                // Sequential insert pattern
                benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 16>>(std::vector<int>{10, 1});
                benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 16>>(std::vector<int>{20, 1});
            } else if (workload_file.find("1m") != std::string::npos) {
                // Random insert pattern
                benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 16>>(std::vector<int>{10, 1});
                benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 16>>(std::vector<int>{20, 1});
            }
        
            // Mixed workloads
            if (workload_file.find("0.900000i") != std::string::npos) {
                // 90% lookup, 10% insert - optimize for lookups
                benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 16>>(std::vector<int>{2, 1});
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{2, 1});
            } else if (workload_file.find("0.100000i") != std::string::npos) {
                // 10% lookup, 90% insert - optimize for inserts
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{20, 1});
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{20, 1});
            }
        }
    }
    
    if (workload_file.find("fb_100M") != std::string::npos) {
        if (workload_file.find("0.000000i") != std::string::npos) {
            // Facebook dataset lookup-only
            benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{2, 1});
            benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{2, 1});
        } else if (workload_file.find("mix") == std::string::npos) {
            if (workload_file.find("0m") != std::string::npos) {
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{10, 1});
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{10, 1});
            }
        } else {
            if (workload_file.find("0.900000i") != std::string::npos) {
                // Facebook 90% lookup workload
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{2, 1});
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{2, 1});
            } else if (workload_file.find("0.100000i") != std::string::npos) {
                // Facebook 90% insert workload
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{20, 1});
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{20, 1});
            }
        }
    }
    
    if (workload_file.find("osmc_100M") != std::string::npos) {
        if (workload_file.find("0.000000i") != std::string::npos) {
            // Osmc dataset lookup-only
            benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{2, 1});
            benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{2, 1});
        } else if (workload_file.find("mix") == std::string::npos) {
            if (workload_file.find("0m") != std::string::npos) {
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{10, 1});
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{10, 1});
            }
        } else {
            if (workload_file.find("0.900000i") != std::string::npos) {
                // Osmc 90% lookup workload
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{2, 1});
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{2, 1});
            } else if (workload_file.find("0.100000i") != std::string::npos) {
                // Osmc 90% insert workload
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{20, 1});
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{20, 1});
            }
        }
    }
}

} // namespace hybrid_benchmark

template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark, 
                                 bool pareto, const std::vector<int>& params) {
    hybrid_benchmark::run_hybrid_benchmark<Searcher>(benchmark, pareto, params);
}

template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark, 
                                 const std::string& filename) {
    hybrid_benchmark::run_hybrid_workload_benchmark<record>(benchmark, filename);
}

// Template instantiations
INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t); 
