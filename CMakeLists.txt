# Minimum CMake version requirement
cmake_minimum_required(VERSION 3.10)

# Project name and configuration
project(WOSD)

# Platform-specific settings
if(UNIX AND NOT APPLE)
    set(LINUX TRUE)
endif()

# Compiler flags for optimization and warnings
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ffast-math -Wall -Wfatal-errors -march=native")

# Check and enable OpenMP support
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-fopenmp HAS_OPENMP)
if (HAS_OPENMP)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")
endif()

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)

# Enable verbose makefile output
set(CMAKE_VERBOSE_MAKEFILE ON)

# Thread library configuration
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Boost library configuration
find_package(Boost REQUIRED COMPONENTS chrono)

# Add dtl subdirectory
add_subdirectory(dtl)

# Mac OS specific include paths
if (${APPLE})
    include_directories(/usr/local/include/)
endif ()

# Source files configuration
set(UTIL_FILES util.h)

# Benchmark source files
set(BENCHMARK_SOURCES
    "benchmarks/benchmark_dynamic_pgm.cc"
    "benchmarks/benchmark_pgm.cc"
    "benchmarks/benchmark_lipp.cc"
    "benchmarks/benchmark_btree.cc"
    "benchmarks/benchmark_hybrid_pgm_lipp.cc")

# Search implementation files
file(GLOB_RECURSE SEARCH_IMPLEMENTATIONS "searches/*.h" "searches/search.cpp")

# Create benchmark executable
add_executable(benchmark benchmark.cc ${UTIL_FILES} ${BENCHMARK_SOURCES} ${SEARCH_IMPLEMENTATIONS})

# Create generate executable
add_executable(generate generate.cc ${UTIL_FILES})

# Set debug configuration
target_compile_definitions(benchmark PRIVATE NDEBUGGING)

# Include directories for benchmark
target_include_directories(benchmark
    PRIVATE "competitors/PGM-index/include"
    PRIVATE "competitors/stx-btree-0.9/include"
    PRIVATE ${Boost_INCLUDE_DIRS})

# Link libraries for benchmark
target_link_libraries(benchmark
    PRIVATE Threads::Threads dtl
    PRIVATE ${Boost_LIBRARIES}
    PRIVATE ${CMAKE_THREAD_LIBS_INIT}
    PRIVATE dl)

# Include directories for generate
target_include_directories(generate PRIVATE competitors/finedex/include)
