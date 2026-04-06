enable_testing()

file(GLOB_RECURSE TEST_SOURCES CONFIGURE_DEPENDS "${PROJECT_SOURCE_DIR}/test/*/*test.cc")

add_executable(unit_test ${TEST_SOURCES})

target_link_libraries(
	unit_test
	PRIVATE
		GTest::gtest_main
		idlekv_core
		third_part
)

target_include_directories(
	unit_test
	PRIVATE
		${PROJECT_SOURCE_DIR}/src
)

target_compile_definitions(
	unit_test
    PRIVATE
        ASIOCHAN_USE_STANDALONE_ASIO=ON
)

add_executable(block_counter_test
    ${PROJECT_SOURCE_DIR}/test/utils/block_counter_test.cc
)

target_link_libraries(
    block_counter_test
    PRIVATE
        GTest::gtest_main
        idlekv_core
        third_part
)

target_include_directories(
    block_counter_test
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

target_compile_definitions(
    block_counter_test
    PRIVATE
        ASIOCHAN_USE_STANDALONE_ASIO=ON
)
