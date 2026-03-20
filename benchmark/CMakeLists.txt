add_executable(index_bench
    ${CMAKE_CURRENT_SOURCE_DIR}/index_bench.cc
)

target_link_libraries(index_bench
    PRIVATE
        idlekv_core
        Threads::Threads
)

target_include_directories(index_bench
    PRIVATE
        ${PROJECT_DIR}/src
)
