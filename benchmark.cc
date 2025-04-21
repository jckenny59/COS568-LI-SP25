#include "benchmark.h"

#include <cstdlib>

// Keep only PGM and B+Tree includes
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

#include "util.h"           // for util::resolve_type
#include "utils/cxxopts.hpp" // for command-line options
#define COMMA ,
using namespace std;

// If only_mode is true (i.e., user passed --only=PGM or BTree),
// then run 'code' only if "tag" matches the given index name.
#define RUN_IF_SELECTED(tag, code) \
  if (!only_mode || (only == (tag))) { \
    code;                              \
  }

#define CONFIGURE_SEARCH(name, func, type, search_class, record)                                           \
  if (search_type == (name) ) {                                                                           \
    tli::Benchmark<type> benchmark(                                                                      \
        filename, ops, num_repeats, through, build, fence, cold_cache,                                    \
        track_errors, csv, num_threads, verify);                                                          \
    func<search_class, record>(benchmark, pareto, params);                                                \
    break;                                                                                                \
  }

// The "default" macro calls your function but no "search_class".
#define CONFIGURE_DEFAULT(func, type, record)                       \
  if (!pareto && params.empty()) {                            \
    tli::Benchmark<type> benchmark(                           \
        filename, ops, num_repeats, through, build, fence,    \
        cold_cache, track_errors, csv, num_threads, verify);  \
    func<record>(benchmark, filename);                        \
    break;                                                    \
  }

#define CONFIGURE_ALL_SEARCHES(func, type, record) \
  CONFIGURE_DEFAULT(func, type, record) \
  CONFIGURE_SEARCH("binary", func, type, BranchingBinarySearch<record>, record);                           \
  CONFIGURE_SEARCH("linear", func, type, LinearSearch<record>, record);                                    \
  CONFIGURE_SEARCH("avx", func, type, LinearAVX<type COMMA record>, record);                               \
  CONFIGURE_SEARCH("interpolation", func, type, InterpolationSearch<record>, record);                      \
  CONFIGURE_SEARCH("exponential", func, type, ExponentialSearch<record>, record);

// 1) Overload that passes a specific search class
template <class SearchClass, int record>
void run_benchmark_64(tli::Benchmark<uint64_t>& benchmark, bool pareto,
                    const std::vector<int>& params) 
{
  RUN_IF_SELECTED("PGM", benchmark_64_pgm<SearchClass>(benchmark, pareto, params));
  RUN_IF_SELECTED("BTree", benchmark_64_btree<SearchClass>(benchmark, pareto, params));
  RUN_IF_SELECTED("DynamicPGM", benchmark_64_dynamic_pgm<SearchClass>(benchmark, pareto, params));
  RUN_IF_SELECTED("LIPP", benchmark_64_lipp(benchmark));
  RUN_IF_SELECTED("HybridPGMLIPP", benchmark_64_hybrid_pgm_lipp<SearchClass>(benchmark, pareto, params));
}

// 2) Overload that doesn't pass a search class
template <int record>
void run_benchmark_64(tli::Benchmark<uint64_t>& benchmark, const std::string& filename)
{
  RUN_IF_SELECTED("PGM", benchmark_64_pgm<record>(benchmark, filename));
  RUN_IF_SELECTED("BTree", benchmark_64_btree<record>(benchmark, filename));
  RUN_IF_SELECTED("DynamicPGM", benchmark_64_dynamic_pgm<record>(benchmark, filename));
  RUN_IF_SELECTED("LIPP", benchmark_64_lipp(benchmark));
  RUN_IF_SELECTED("HybridPGMLIPP", benchmark_64_hybrid_pgm_lipp<record>(benchmark, filename));
}

// We don't do string benchmarks in this minimal build
template <class SearchClass, int record>
void run_benchmark_string(tli::Benchmark<std::string>&, bool, const std::vector<int>&) {}
template <int record>
void run_benchmark_string(tli::Benchmark<std::string>&, const std::string&) {}

int main(int argc, char* argv[]) {
  cxxopts::Options options("benchmark", "Searching on sorted data benchmark");
  options.positional_help("<data> <ops>");
  options.add_options()
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

  options.parse_positional({"data", "ops"});

  auto result = options.parse(argc, argv);

  if (result.count("help")) {
    std::cout << options.help({}) << "\n";
    return 0;
  }

  bool through = result.count("through");
  size_t num_repeats = through ? result["repeats"].as<int>() : 1;
  std::cout << "Repeating lookup code " << num_repeats << " time(s)." << std::endl;

  size_t num_threads = result["threads"].as<int>();
  std::cout << "Using " << num_threads << " thread(s)." << std::endl;

  bool build        = result.count("build");
  bool fence        = result.count("fence");
  bool track_errors = result.count("errors");
  bool verify       = result.count("verify");
  bool cold_cache   = result.count("cold-cache");
  bool csv          = result.count("csv");
  bool pareto       = result.count("pareto");

  std::string filename   = result["data"].as<std::string>();
  std::string ops        = result["ops"].as<std::string>();
  std::string search_type= result["search"].as<std::string>();

  bool only_mode = result.count("only") || std::getenv("TLI_ONLY");
  std::vector<int> params = result["params"].as<std::vector<int>>();

  std::string only;
  if (result.count("only")) {
    only = result["only"].as<std::string>();
  } else if (std::getenv("TLI_ONLY")) {
    only = std::string(std::getenv("TLI_ONLY"));
  } else {
    only = "";
  }

  DataType type = util::resolve_type(filename);

  switch (type) {
    case DataType::UINT64: {
      // Create benchmark.
      if (track_errors){
        if (num_threads > 1){
          CONFIGURE_ALL_SEARCHES(run_benchmark_64, uint64_t, 2);
        } else {
          CONFIGURE_ALL_SEARCHES(run_benchmark_64, uint64_t, 1);
        }
      }
      else{
        CONFIGURE_ALL_SEARCHES(run_benchmark_64, uint64_t, 0);
      }
      break;
    }

    case DataType::STRING: {
      // Create benchmark.
      if (track_errors){
        if (num_threads > 1){
          CONFIGURE_ALL_SEARCHES(run_benchmark_string, std::string, 2);
        } else{
          CONFIGURE_ALL_SEARCHES(run_benchmark_string, std::string, 1);
        }
      }
      else{
        CONFIGURE_ALL_SEARCHES(run_benchmark_string, std::string, 0);
      }
      break;
    }
  }

  return 0;
}
