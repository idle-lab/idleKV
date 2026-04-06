cmake_minimum_required(VERSION 3.17)
project(idlekv)

option(ENABLE_TEST "Compile unit tests or not" ON)
option(ENABLE_BENCHMARK "Compile benchmark tests or not" ON)
option(DISABLE_CHECK "Disable runtime CHECK macros" ON)

include(cmake/utils.cmake)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to `Debug` as none was specified.")
    set(CMAKE_BUILD_TYPE "Debug")
endif()

# Prefer the system Boost toolchain over any active Conda environment so the
# linked C++ runtime matches /usr/bin/g++.
if(DEFINED ENV{CONDA_PREFIX} AND
   EXISTS "/usr/lib/x86_64-linux-gnu/cmake/Boost-1.83.0/BoostConfig.cmake")
    set(Boost_DIR "/usr/lib/x86_64-linux-gnu/cmake/Boost-1.83.0"
        CACHE PATH "Preferred Boost config" FORCE)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(PROJECT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
# 编译选项
if (MSVC)
    add_compile_options(/W4 /utf-8)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Zi /Od /DDEBUG")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2 /DNDEBUG")
else()
    add_compile_options(-Wall -Wextra)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g3 -O0 -DDEBUG")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -march=native -DNDEBUG")
endif()

# 查找依赖
find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS fiber context system thread)
find_library(XXHASH_LIBRARY xxhash REQUIRED)

add_subdirectory(third_part)
add_subdirectory(src)
if (ENABLE_BENCHMARK)
    add_subdirectory(benchmark)
endif()
if (ENABLE_TEST)
    add_subdirectory(test)
endif()
