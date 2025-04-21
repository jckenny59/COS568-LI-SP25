#pragma once

#include "benchmark.h"

// Forward declarations for hybrid PGM-LIPP benchmark functions
namespace hybrid_benchmark {

// Benchmark function for hybrid index with configurable parameters
template <typename SearchStrategy>
void run_hybrid_benchmark(tli::Benchmark<uint64_t>& benchmark, 
                         bool pareto_mode, 
                         const std::vector<int>& parameters);

// Benchmark function for specific record type and workload
template <int RecordType>
void run_hybrid_workload_benchmark(tli::Benchmark<uint64_t>& benchmark, 
                                  const std::string& workload_file);

} // namespace hybrid_benchmark

// Alias the functions to maintain compatibility with existing code
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