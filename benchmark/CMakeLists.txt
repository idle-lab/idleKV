add_executable(index_bench
    ${CMAKE_CURRENT_SOURCE_DIR}/index_bench.cc
    ${PROJECT_SOURCE_DIR}/src/db/storage/art/node.cc
)

target_link_libraries(index_bench
    PRIVATE
        third_part
        absl::flat_hash_map
        Threads::Threads
        ${XXHASH_LIBRARY}
)

target_include_directories(index_bench
    PRIVATE
        ${PROJECT_DIR}/src
)

if (DISABLE_CHECK)
    # target_compile_definitions(index_bench
    #     PRIVATE
    #         DISABLE_CHECK=1
    # )
endif()
