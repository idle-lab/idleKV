add_library(third_part INTERFACE)

target_include_directories(third_part
    INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}
        asio
)

add_subdirectory(spdlog)
add_subdirectory(asiochan)

if (ENABLE_TEST)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE) # overriding the parent project's compiler/linker settings
    add_subdirectory(gtest)
endif()

target_link_libraries(third_part
    INTERFACE
        spdlog::spdlog_header_only
        asiochan::asiochan
)