cmake_minimum_required(VERSION 3.17)
project(idlekv)

option(ENABLE_TEST "是否编译单元测试" ON)

include(cmake/utils.cmake)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
        message(STATUS "Setting build type to `Debug` as none was specified.")
        set(CMAKE_BUILD_TYPE "Debug")
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
    add_compile_options(-Wall -Wextra -O3 -march=native)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -DDEBUG")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")
endif()

# 查找依赖
find_package(Threads REQUIRED)

add_subdirectory(third_part)
add_subdirectory(src)
if (ENABLE_TEST)
    add_subdirectory(test)
endif()
