enable_testing()

file(GLOB_RECURSE TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*/*test.cc")

add_executable(unit_test ${TEST_SOURCES})

target_link_libraries(
	unit_test
	GTest::gtest_main
)

target_include_directories(
	unit_test
	PRIVATE
		${PROJECT_SOURCE_DIR}/src
)
