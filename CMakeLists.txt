cmake_minimum_required(VERSION 3.17)
project(idlekv)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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

add_library(third_party INTERFACE)

target_include_directories(third_party 
    INTERFACE
        third_part
        third_part/asio
)

add_subdirectory(third_part/spdlog)
add_subdirectory(third_part/asiochan)

target_link_libraries(third_party
    INTERFACE
        spdlog::spdlog_header_only
        asiochan::asiochan
)

set(PROJECT_DIR ${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(src/)

