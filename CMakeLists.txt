cmake_minimum_required(VERSION 3.11)
project(idleKV)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 编译选项
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O3 -march=native")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")

# 查找依赖
find_package(Threads REQUIRED)

# 包含目录
include_directories("${CMAKE_SOURCE_DIR}/src")

# 源文件
set(CORE_SOURCES
    # 索引层
    src/index/art.cc
)

# 创建核心库
add_library(idlekv_core STATIC ${CORE_SOURCES})
target_link_libraries(idlekv_core Threads::Threads)


# 主程序
add_executable(idlekv main.cc)
target_link_libraries(idlekv idlekv_core)
