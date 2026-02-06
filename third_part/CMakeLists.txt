add_library(third_party INTERFACE)

target_include_directories(third_party 
    INTERFACE
        third_part
        third_part/asio
)

add_subdirectory(spdlog)
add_subdirectory(asiochan)

if (ENABLE_TEST)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE) # overriding the parent project's compiler/linker settings
    add_subdirectory(gtest)
endif()

target_link_libraries(third_party
    INTERFACE
        spdlog::spdlog_header_only
        asiochan::asiochan
)