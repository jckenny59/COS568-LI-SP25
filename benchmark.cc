#include "benchmark.h"

#include <cstdlib>

// Include benchmark headers for different index implementations
#include "benchmarks/benchmark_btree.h"
#include "benchmarks/benchmark_pgm.h"
#include "benchmarks/benchmark_dynamic_pgm.h"
#include "benchmarks/benchmark_lipp.h"
#include "benchmarks/benchmark_hybrid_pgm_lipp.h"

// Include search algorithm implementations
#include "searches/linear_search.h"
#include "searches/linear_search_avx.h"
#include "searches/branching_binary_search.h"
#include "searches/exponential_search.h"
#include "searches/interpolation_search.h"

#include "util.h"
#include "utils/cxxopts.hpp"

using namespace std;

// Macro to conditionally execute code based on index selection
#define RUN_IF_SELECTED(index_name, code) \
  if (!index_filter_enabled || (selected_index == (index_name))) { \
    code; \
  }

// Macro to add search type configuration
#define CONFIGURE_SEARCH(name, benchmark_func, key_type, search_impl, record_type) \
  if (search_algorithm == (name)) { \
    tli::Benchmark<key_type> benchmark_config( \
        data_file, operations, repeat_count, measure_throughput, build_only, \
        memory_fence, clear_cache, track_errors, csv_output, thread_count, verify_results); \
    benchmark_func<search_impl, record_type>(benchmark_config, pareto_mode, parameters, \
        index_filter_enabled, selected_index, data_file); \
    break; \
  }

// Macro for default configuration without specific search implementation
#define CONFIGURE_DEFAULT(benchmark_func, key_type, record_type) \
  if (!pareto_mode && parameters.empty()) { \
    tli::Benchmark<key_type> benchmark_config( \
        data_file, operations, repeat_count, measure_throughput, build_only, \
        memory_fence, clear_cache, track_errors, csv_output, thread_count, verify_results); \
    benchmark_func<record_type>(benchmark_config, index_filter_enabled, selected_index, operations); \
    break; \
  }

// Macro to configure all search types
#define CONFIGURE_ALL_SEARCHES(benchmark_func, key_type, record_type, index_filter_enabled, selected_index) \
  CONFIGURE_DEFAULT(benchmark_func, key_type, record_type) \
  CONFIGURE_SEARCH("binary", benchmark_func, key_type, BranchingBinarySearch<record_type>, record_type) \
  CONFIGURE_SEARCH("linear", benchmark_func, key_type, LinearSearch<record_type>, record_type) \
  CONFIGURE_SEARCH("avx", benchmark_func, key_type, LinearAVX<key_type, record_type>, record_type) \
  CONFIGURE_SEARCH("interpolation", benchmark_func, key_type, InterpolationSearch<record_type>, record_type) \
  CONFIGURE_SEARCH("exponential", benchmark_func, key_type, ExponentialSearch<record_type>, record_type)

// Template function for 64-bit key benchmarks with specific search implementation
template <class SearchImplementation, int RecordType>
void run_benchmark_64(tli::Benchmark<uint64_t>& benchmark_config, bool pareto_mode,
                     const std::vector<int>& parameters, bool index_filter_enabled,
                     const std::string& selected_index, const std::string& /*data_file*/) {
  RUN_IF_SELECTED("PGM", benchmark_64_pgm<SearchImplementation>(benchmark_config, pareto_mode, parameters));
  RUN_IF_SELECTED("BTree", benchmark_64_btree<SearchImplementation>(benchmark_config, pareto_mode, parameters));
  RUN_IF_SELECTED("DynamicPGM", benchmark_64_dynamic_pgm<SearchImplementation>(benchmark_config, pareto_mode, parameters));
  RUN_IF_SELECTED("LIPP", benchmark_64_lipp(benchmark_config));
  RUN_IF_SELECTED("HybridPGMLIPP", benchmark_64_hybrid_pgm_lipp<SearchImplementation>(benchmark_config, pareto_mode, parameters));
}

// Template function for 64-bit key benchmarks without specific search implementation
template <int RecordType>
void run_benchmark_64(tli::Benchmark<uint64_t>& benchmark_config, bool index_filter_enabled,
                     const std::string& selected_index, const std::string& data_file) {
  RUN_IF_SELECTED("PGM", benchmark_64_pgm<RecordType>(benchmark_config, data_file));
  RUN_IF_SELECTED("BTree", benchmark_64_btree<RecordType>(benchmark_config, data_file));
  RUN_IF_SELECTED("DynamicPGM", benchmark_64_dynamic_pgm<RecordType>(benchmark_config, data_file));
  RUN_IF_SELECTED("LIPP", benchmark_64_lipp(benchmark_config));
  RUN_IF_SELECTED("HybridPGMLIPP", benchmark_64_hybrid_pgm_lipp<RecordType>(benchmark_config, data_file));
}

// Empty implementations for string benchmarks (not used in minimal build)
template <class SearchImplementation, int RecordType>
void run_benchmark_string(tli::Benchmark<std::string>&, bool, const std::vector<int>&,
                         bool, const std::string&, const std::string&) {}
template <int RecordType>
void run_benchmark_string(tli::Benchmark<std::string>&, bool,
                         const std::string&, const std::string&) {}

int main(int argc, char* argv[]) {
  cxxopts::Options cli_options("benchmark", "Benchmark for searching on sorted data");
  cli_options.positional_help("<data> <ops>");
  cli_options.add_options()
    ("data", "Input data file containing keys", cxxopts::value<std::string>())
    ("ops", "Workload file containing operations", cxxopts::value<std::string>())
    ("help", "Display help information")
    ("t,threads", "Number of threads for lookups", cxxopts::value<int>()->default_value("1"))
    ("through", "Measure and report throughput")
    ("r,repeats", "Number of benchmark repetitions", cxxopts::value<int>()->default_value("1"))
    ("b,build", "Measure only build times")
    ("only", "Run only specified index", cxxopts::value<std::string>()->default_value(""))
    ("cold-cache", "Clear CPU cache between lookups")
    ("pareto", "Run with multiple size configurations")
    ("fence", "Execute memory barrier between lookups")
    ("errors", "Track and report index errors")
    ("verify", "Verify execution correctness")
    ("csv", "Output results in CSV format")
    ("search", "Search algorithm type", 
       cxxopts::value<std::string>()->default_value("binary"))
    ("params", "Index parameters", cxxopts::value<std::vector<int>>()->default_value(""));

  cli_options.parse_positional({"data", "ops"});

  auto cli_result = cli_options.parse(argc, argv);

  if (cli_result.count("help")) {
    std::cout << cli_options.help({}) << "\n";
    return 0;
  }

  bool measure_throughput = cli_result.count("through");
  size_t repeat_count = measure_throughput ? cli_result["repeats"].as<int>() : 1;
  std::cout << "Executing benchmark " << repeat_count << " time(s)." << std::endl;

  size_t thread_count = cli_result["threads"].as<int>();
  std::cout << "Using " << thread_count << " thread(s)." << std::endl;

  bool build_only = cli_result.count("build");
  bool memory_fence = cli_result.count("fence");
  bool track_errors = cli_result.count("errors");
  bool verify_results = cli_result.count("verify");
  bool clear_cache = cli_result.count("cold-cache");
  bool csv_output = cli_result.count("csv");
  bool pareto_mode = cli_result.count("pareto");

  std::string data_file = cli_result["data"].as<std::string>();
  std::string operations = cli_result["ops"].as<std::string>();
  std::string search_algorithm = cli_result["search"].as<std::string>();

  bool index_filter_enabled = cli_result.count("only") || std::getenv("TLI_ONLY");
  std::vector<int> parameters = cli_result["params"].as<std::vector<int>>();

  std::string selected_index;
  if (cli_result.count("only")) {
    selected_index = cli_result["only"].as<std::string>();
  } else if (std::getenv("TLI_ONLY")) {
    selected_index = std::string(std::getenv("TLI_ONLY"));
  } else {
    selected_index = "";
  }

  DataType key_type = util::resolve_type(data_file);

  switch (key_type) {
    case DataType::UINT64: {
      if (track_errors) {
        if (thread_count > 1) {
          CONFIGURE_ALL_SEARCHES(run_benchmark_64, uint64_t, 2, index_filter_enabled, selected_index);
        } else {
          CONFIGURE_ALL_SEARCHES(run_benchmark_64, uint64_t, 1, index_filter_enabled, selected_index);
        }
      } else {
        CONFIGURE_ALL_SEARCHES(run_benchmark_64, uint64_t, 0, index_filter_enabled, selected_index);
      }
      break;
    }

    case DataType::STRING: {
      if (track_errors) {
        if (thread_count > 1) {
          CONFIGURE_ALL_SEARCHES(run_benchmark_string, std::string, 2, index_filter_enabled, selected_index);
        } else {
          CONFIGURE_ALL_SEARCHES(run_benchmark_string, std::string, 1, index_filter_enabled, selected_index);
        }
      } else {
        CONFIGURE_ALL_SEARCHES(run_benchmark_string, std::string, 0, index_filter_enabled, selected_index);
      }
      break;
    }
  }

  // Update the macro calls to include all required parameters
  CONFIGURE_ALL_SEARCHES(run_benchmark_64, uint64_t, 2, false, 0);
  CONFIGURE_ALL_SEARCHES(run_benchmark_64, uint64_t, 2, true, 0);
  CONFIGURE_ALL_SEARCHES(run_benchmark_64, uint64_t, 2, false, 1);
  CONFIGURE_ALL_SEARCHES(run_benchmark_64, uint64_t, 2, true, 1);
  CONFIGURE_ALL_SEARCHES(run_benchmark_64, uint64_t, 2, false, 2);
  CONFIGURE_ALL_SEARCHES(run_benchmark_64, uint64_t, 2, true, 2);

  return 0;
}
