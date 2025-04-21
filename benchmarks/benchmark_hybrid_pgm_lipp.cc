#include "benchmarks/benchmark_hybrid_pgm_lipp.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark, 
                                 bool pareto, const std::vector<int>& params) {
    if (!pareto) {
        // Default configuration with 5% migration threshold
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{5});
    } else {
        // Test different migration thresholds
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{1});
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{5});
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{10});
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{20});
    }
}

template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark, 
                                 const std::string& filename) {
    // Default configuration for all datasets
    benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{5});
    benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{5});
    benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 16>>(std::vector<int>{5});
    benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 16>>(std::vector<int>{5});
}

// Template instantiations for record=0
template void benchmark_64_hybrid_pgm_lipp<LinearSearch<0>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<BranchingBinarySearch<0>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<InterpolationSearch<0>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<ExponentialSearch<0>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<LinearAVX<uint64_t, 0>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<0>(tli::Benchmark<uint64_t>&, const std::string&);

// Template instantiations for record=1
template void benchmark_64_hybrid_pgm_lipp<LinearSearch<1>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<BranchingBinarySearch<1>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<InterpolationSearch<1>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<ExponentialSearch<1>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<LinearAVX<uint64_t, 1>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<1>(tli::Benchmark<uint64_t>&, const std::string&);

// Template instantiations for record=2
template void benchmark_64_hybrid_pgm_lipp<LinearSearch<2>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<BranchingBinarySearch<2>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<InterpolationSearch<2>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<ExponentialSearch<2>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<LinearAVX<uint64_t, 2>>(tli::Benchmark<uint64_t>&, bool, const std::vector<int>&);
template void benchmark_64_hybrid_pgm_lipp<2>(tli::Benchmark<uint64_t>&, const std::string&); 