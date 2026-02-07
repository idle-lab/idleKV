enable_testing()

file(GLOB_RECURSE TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*/*test.cc")

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
