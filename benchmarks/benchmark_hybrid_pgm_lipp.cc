#include "benchmarks/benchmark_hybrid_pgm_lipp.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

namespace hybrid_benchmark {

template <typename SearchStrategy>
void run_hybrid_benchmark(tli::Benchmark<uint64_t>& benchmark, 
                         bool pareto_mode, 
                         const std::vector<int>& parameters) {
    if (!pareto_mode) {
        // Default configuration with 5% migration threshold for initial testing
        benchmark.template Run<HybridPGMLIPP<uint64_t, SearchStrategy, 16>>(std::vector<int>{5});
    } else {
        // Test different migration thresholds for performance analysis
        benchmark.template Run<HybridPGMLIPP<uint64_t, SearchStrategy, 16>>(std::vector<int>{1});  // 1% threshold
        benchmark.template Run<HybridPGMLIPP<uint64_t, SearchStrategy, 16>>(std::vector<int>{5});  // 5% threshold
        benchmark.template Run<HybridPGMLIPP<uint64_t, SearchStrategy, 16>>(std::vector<int>{10}); // 10% threshold
        benchmark.template Run<HybridPGMLIPP<uint64_t, SearchStrategy, 16>>(std::vector<int>{20}); // 20% threshold
    }
}

template <int RecordType>
void run_hybrid_workload_benchmark(tli::Benchmark<uint64_t>& benchmark, 
                                  const std::string& workload_file) {
    // Handle different datasets and workload types
    if (workload_file.find("books_100M") != std::string::npos) {
        if (workload_file.find("0.000000i") != std::string::npos) {
            // Lookup-only workload - prioritize fast lookups
            benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<RecordType>, 16>>(std::vector<int>{5});
            benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<RecordType>, 16>>(std::vector<int>{5});
        } else if (workload_file.find("mix") == std::string::npos) {
            if (workload_file.find("0m") != std::string::npos) {
                // Sequential insert pattern
                benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<RecordType>, 16>>(std::vector<int>{5});
                benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<RecordType>, 16>>(std::vector<int>{10});
            } else if (workload_file.find("1m") != std::string::npos) {
                // Random insert pattern
                benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<RecordType>, 16>>(std::vector<int>{5});
                benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<RecordType>, 16>>(std::vector<int>{10});
            }
        } else {
            // Mixed workloads
            if (workload_file.find("0.900000i") != std::string::npos) {
                // 90% lookup, 10% insert - optimize for lookups
                benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<RecordType>, 16>>(std::vector<int>{5});
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<RecordType>, 16>>(std::vector<int>{5});
            } else if (workload_file.find("0.100000i") != std::string::npos) {
                // 10% lookup, 90% insert - optimize for inserts
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<RecordType>, 16>>(std::vector<int>{10});
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<RecordType>, 16>>(std::vector<int>{10});
            }
        }
    }
    
    if (workload_file.find("fb_100M") != std::string::npos) {
        if (workload_file.find("0.000000i") != std::string::npos) {
            // Facebook dataset lookup-only
            benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<RecordType>, 16>>(std::vector<int>{5});
            benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<RecordType>, 16>>(std::vector<int>{5});
        } else if (workload_file.find("mix") == std::string::npos) {
            if (workload_file.find("0m") != std::string::npos) {
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<RecordType>, 16>>(std::vector<int>{5});
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<RecordType>, 16>>(std::vector<int>{5});
            }
        } else {
            if (workload_file.find("0.900000i") != std::string::npos) {
                // Facebook 90% lookup workload
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<RecordType>, 16>>(std::vector<int>{5});
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<RecordType>, 16>>(std::vector<int>{5});
            } else if (workload_file.find("0.100000i") != std::string::npos) {
                // Facebook 90% insert workload
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<RecordType>, 16>>(std::vector<int>{10});
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<RecordType>, 16>>(std::vector<int>{10});
            }
        }
    }
    
    if (workload_file.find("osmc_100M") != std::string::npos) {
        if (workload_file.find("0.000000i") != std::string::npos) {
            // Osmc dataset lookup-only
            benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<RecordType>, 16>>(std::vector<int>{5});
            benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<RecordType>, 16>>(std::vector<int>{5});
        } else if (workload_file.find("mix") == std::string::npos) {
            if (workload_file.find("0m") != std::string::npos) {
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<RecordType>, 16>>(std::vector<int>{5});
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<RecordType>, 16>>(std::vector<int>{5});
            }
        } else {
            if (workload_file.find("0.900000i") != std::string::npos) {
                // Osmc 90% lookup workload
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<RecordType>, 16>>(std::vector<int>{5});
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<RecordType>, 16>>(std::vector<int>{5});
            } else if (workload_file.find("0.100000i") != std::string::npos) {
                // Osmc 90% insert workload
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<RecordType>, 16>>(std::vector<int>{10});
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<RecordType>, 16>>(std::vector<int>{10});
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

// Explicit template instantiations for different search types
template void benchmark_64_hybrid_pgm_lipp<BranchingBinarySearch<uint64_t>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<LinearSearch<uint64_t>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<InterpolationSearch<uint64_t>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<ExponentialSearch<uint64_t>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);

// Template instantiations for record types
template void benchmark_64_hybrid_pgm_lipp<0>(tli::Benchmark<uint64_t>&, const std::string&);
template void benchmark_64_hybrid_pgm_lipp<1>(tli::Benchmark<uint64_t>&, const std::string&);
template void benchmark_64_hybrid_pgm_lipp<2>(tli::Benchmark<uint64_t>&, const std::string&); 