#!/bin/bash
# Script to generate a minimal CMake configuration file

# Exit immediately if any command fails
set -e

# Function to create backup of original CMakeLists.txt
create_backup() {
    if [ ! -f CMakeLists.txt.original ]; then
        echo "Creating backup of original CMakeLists.txt..."
        cp CMakeLists.txt CMakeLists.txt.original
        echo "Backup saved as CMakeLists.txt.original"
    else
        echo "Backup already exists: CMakeLists.txt.original"
    fi
}

# Function to generate minimal CMake configuration
generate_cmake_config() {
    echo "Generating minimal CMake configuration..."
    cat > CMakeLists.txt << 'EOF'
# Minimum required CMake version
cmake_minimum_required(VERSION 3.10)

# Project configuration
project(WOSD)

# Platform-specific settings
if(UNIX AND NOT APPLE)
    set(LINUX TRUE)
endif()

# Compiler flags configuration
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ffast-math -Wall -Wfatal-errors -march=native")

# OpenMP support check and configuration
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-fopenmp OPENMP_SUPPORTED)
if (OPENMP_SUPPORTED)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")
endif()

# C++ standard configuration
set(CMAKE_CXX_STANDARD 17)

# Thread and Boost dependencies
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS chrono)

# Include dtl subdirectory
add_subdirectory(dtl)

# macOS specific include paths
if (${APPLE})
    include_directories(/usr/local/include/)
endif ()

# Source file configuration
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

# Executable targets
add_executable(benchmark benchmark.cc ${UTIL_FILES} ${BENCHMARK_SOURCES} ${SEARCH_IMPLEMENTATIONS})
add_executable(generate generate.cc ${UTIL_FILES})

# Benchmark target configuration
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

# Generate target configuration
target_include_directories(generate PRIVATE competitors/finedex/include)
EOF
}

# Main script execution
create_backup
generate_cmake_config
echo "Minimal CMake configuration generated successfully!"
