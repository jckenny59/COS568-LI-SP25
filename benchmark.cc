#include "benchmark.h"

#include <cstdlib>

// Core benchmark implementations
#include "benchmarks/benchmark_btree.h"
#include "benchmarks/benchmark_pgm.h"
#include "benchmarks/benchmark_dynamic_pgm.h"
#include "benchmarks/benchmark_lipp.h"
#include "benchmarks/benchmark_hybrid_pgm_lipp.h"

#include "searches/linear_search.h"
#include "searches/linear_search_avx.h"
#include "searches/branching_binary_search.h"
#include "searches/exponential_search.h"
#include "searches/interpolation_search.h"

#include "util.h"
#include "utils/cxxopts.hpp"

#define COMMA ,
using namespace std;

// If selection_mode is true (i.e., user passed --only=PGM or BTree),
// then run 'code' only if "tag" matches the given index name.
#define execute_if_selected(tag, code) \
  if (!selection_mode || (selected_index == (tag))) { \
    code;                              \
  }

#define configure_search_implementation(name, func, type, search_class, record)                                           \
  if (search_algorithm == (name) ) {                                                                           \
    tli::Benchmark<type> benchmark_config(                                                                      \
        filename, operations, repetition_count, throughput_mode, build_phase, memory_fence, cache_clear,                                    \
        error_tracking, csv_output, thread_count, verification_mode);                                                          \
    func<search_class, record>(benchmark_config, pareto_analysis, configuration_params, selection_mode, selected_index, filename);                     \
    break;                                                                                                \
  }

// The "default" macro calls your function but no "search_class".
#define setup_default_benchmark(func, type, record)                       \
  if (!pareto_analysis && configuration_params.empty()) {                            \
    tli::Benchmark<type> benchmark_config(                           \
        filename, operations, repetition_count, throughput_mode, build_phase, memory_fence,    \
        cache_clear, error_tracking, csv_output, thread_count, verification_mode);  \
    func<record>(benchmark_config, selection_mode, selected_index, operations);            \
    break;                                                    \
  }


#define add_search_types(func, type, record) \
  setup_default_benchmark(func, type, record) \
  configure_search_implementation("binary", func, type, BranchingBinarySearch<record>, record);                           \
  configure_search_implementation("linear", func, type, LinearSearch<record>, record);                                    \
  configure_search_implementation("avx", func, type, LinearAVX<type COMMA record>, record);                               \
  configure_search_implementation("interpolation", func, type, InterpolationSearch<record>, record);                      \
  configure_search_implementation("exponential", func, type, ExponentialSearch<record>, record);

// 1) Overload that passes a specific search class
template <class SearchClass, int record>
void run_uint64_benchmark(tli::Benchmark<uint64_t>& benchmark_config, bool pareto_analysis,
                    const std::vector<int>& configuration_params, bool selection_mode,
                    const std::string& selected_index, const std::string& /*filename*/) 
{
  // Only run if user specifies --only=PGM or --only=BTree.
  execute_if_selected("PGM", benchmark_64_pgm<SearchClass>(benchmark_config, pareto_analysis, configuration_params));
  execute_if_selected("BTree", benchmark_64_btree<SearchClass>(benchmark_config, pareto_analysis, configuration_params));
  execute_if_selected("DynamicPGM", benchmark_64_dynamic_pgm<SearchClass>(benchmark_config, pareto_analysis, configuration_params));
  execute_if_selected("LIPP", benchmark_64_lipp(benchmark_config));
  execute_if_selected("HybridPGMLIPP", benchmark_64_hybrid_pgm_lipp<SearchClass>(benchmark_config, pareto_analysis, configuration_params));
}

// 2) Overload that doesn't pass a search class
template <int record>
void run_uint64_benchmark(tli::Benchmark<uint64_t>& benchmark_config, bool selection_mode,
                    const std::string& selected_index, const std::string& filename)
{
  // Only run if user specifies --only=PGM or --only=BTree
  execute_if_selected("PGM", benchmark_64_pgm<record>(benchmark_config, filename));
  execute_if_selected("BTree", benchmark_64_btree<record>(benchmark_config, filename));
  execute_if_selected("DynamicPGM", benchmark_64_dynamic_pgm<record>(benchmark_config, filename));
  execute_if_selected("LIPP", benchmark_64_lipp(benchmark_config));
  execute_if_selected("HybridPGMLIPP", benchmark_64_hybrid_pgm_lipp<record>(benchmark_config, filename));
}

// We don't do string benchmarks in this minimal build
template <class SearchClass, int record>
void run_string_benchmark(tli::Benchmark<std::string>&, bool, const std::vector<int>&,
                    bool, const std::string&, const std::string&) {}
template <int record>
void run_string_benchmark(tli::Benchmark<std::string>&, bool,
                    const std::string&, const std::string&) {}

int benchmark_main(int argc, char* argv[]) {
  cxxopts::Options cli_options("benchmark", "Searching on sorted data benchmark");
  cli_options.positional_help("<data> <ops>");
  cli_options.add_options()
    ("data", "Data file with keys", cxxopts::value<std::string>())
    ("ops", "Workload file with operations", cxxopts::value<std::string>())
    ("help", "Displays help")
    ("t,threads", "Number of lookup threads", cxxopts::value<int>()->default_value("1"))
    ("through", "Measure throughput")
    ("r,repeats", "Number of repeats", cxxopts::value<int>()->default_value("1"))
    ("b,build", "Only measure and report build times")
    ("only", "Only run the specified index", cxxopts::value<std::string>()->default_value(""))
    ("cold-cache", "Clear the CPU cache between each lookup")
    ("pareto", "Run with multiple different sizes for each competitor")
    ("fence", "Execute a memory barrier between each lookup")
    ("errors", "Tracks index errors, and report those instead of lookup times")
    ("verify", "Verify correctness of execution")
    ("csv", "Output a CSV of results in addition to a text file")
    ("search", "Specify a search type (binary, linear, etc.)", 
       cxxopts::value<std::string>()->default_value("binary"))
    ("params", "Set parameters of index", cxxopts::value<std::vector<int>>()->default_value(""));

  cli_options.parse_positional({"data", "ops"});

  auto cli_result = cli_options.parse(argc, argv);

  if (cli_result.count("help")) {
    std::cout << cli_options.help({}) << "\n";
    return 0;
  }

  bool throughput_mode = cli_result.count("through");
  size_t repetition_count = throughput_mode ? cli_result["repeats"].as<int>() : 1;
  std::cout << "Executing benchmark with " << repetition_count << " repetition(s)." << std::endl;

  size_t thread_count = cli_result["threads"].as<int>();
  std::cout << "Utilizing " << thread_count << " thread(s)." << std::endl;

  bool build_phase        = cli_result.count("build");
  bool memory_fence        = cli_result.count("fence");
  bool error_tracking = cli_result.count("errors");
  bool verification_mode       = cli_result.count("verify");
  bool cache_clear   = cli_result.count("cold-cache");
  bool csv_output          = cli_result.count("csv");
  bool pareto_analysis       = cli_result.count("pareto");

  std::string filename   = cli_result["data"].as<std::string>();
  std::string operations        = cli_result["ops"].as<std::string>();
  std::string search_algorithm= cli_result["search"].as<std::string>();

  bool selection_mode = cli_result.count("only") || std::getenv("TLI_ONLY");
  std::vector<int> configuration_params = cli_result["params"].as<std::vector<int>>();

  std::string selected_index;
  if (cli_result.count("only")) {
    selected_index = cli_result["only"].as<std::string>();
  } else if (std::getenv("TLI_ONLY")) {
    selected_index = std::string(std::getenv("TLI_ONLY"));
  } else {
    selected_index = "";
  }

  DataType data_type = util::resolve_type(filename);

  switch (data_type) {
    case DataType::UINT64: {
      // Create benchmark.
      if (error_tracking){
        if (thread_count > 1){
          add_search_types(run_uint64_benchmark, uint64_t, 2);
        } else {
          add_search_types(run_uint64_benchmark, uint64_t, 1);
        }
      }
      else{
        add_search_types(run_uint64_benchmark, uint64_t, 0);
      }
      break;
    }

    case DataType::STRING: {
      // Create benchmark.
      if (error_tracking){
        if (thread_count > 1){
          add_search_types(run_string_benchmark, std::string, 2);
        } else{
          add_search_types(run_string_benchmark, std::string, 1);
        }
      }
      else{
        add_search_types(run_string_benchmark, std::string, 0);
      }
      break;
    }
  }

  return 0;
}