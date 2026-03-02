file(GLOB_RECURSE SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cc"
)

# main.cc is the executable entry and should not be part of idlekv_core.
list(REMOVE_ITEM SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/main.cc"
)


add_library(idlekv_core ${SOURCES})

target_include_directories(idlekv_core
    PUBLIC
        ${PROJECT_DIR}/src
)

target_link_libraries(idlekv_core
    PUBLIC
        third_part
)

target_compile_definitions(idlekv_core
    PUBLIC
        ASIOCHAN_USE_STANDALONE_ASIO=ON
)

add_executable(idlekv "${CMAKE_CURRENT_SOURCE_DIR}/main.cc")

target_link_libraries(idlekv
    PRIVATE
        idlekv_core
)
