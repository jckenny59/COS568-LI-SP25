#include "benchmarks/benchmark_hybrid_pgm_lipp.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark, 
                                 bool pareto, const std::vector<int>& params) {
    if (!pareto) {
        util::fail("Hybrid PGM+LIPP's hyperparameter cannot be set");
    } else {
        // Test with different error bounds for PGM component
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 8>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 32>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 64>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 128>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 256>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 512>>();
    }
}

template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark, 
                                 const std::string& filename) {
    if (filename.find("fb_100M") != std::string::npos) {
        if (filename.find("0.000000i") != std::string::npos) {
            // Lookup-only workload
            benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>();
            benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>();
            benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 64>>();
            benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 128>>();
        } else if (filename.find("mix") != std::string::npos) {
            if (filename.find("0.100000i") != std::string::npos) {
                // 10% insert, 90% lookup
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>();
                benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 256>>();
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 64>>();
                benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 512>>();
            } else if (filename.find("0.900000i") != std::string::npos) {
                // 90% insert, 10% lookup
                benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 256>>();
                benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>();
                benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 64>>();
                benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 512>>();
            }
        }
    }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t); 