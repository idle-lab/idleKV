cmake_minimum_required(VERSION 3.11)
project(idlekv)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 编译选项
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O3 -march=native")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")

# 查找依赖
find_package(Threads REQUIRED)

add_library(third_party INTERFACE)

target_include_directories(third_party 
    INTERFACE
        third_part
        third_part/asio
)

add_subdirectory(third_part/spdlog)

target_link_libraries(third_party
    INTERFACE
        spdlog::spdlog_header_only
)

set(PROJECT_DIR ${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(src/)

