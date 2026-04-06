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
        Boost::fiber
        Boost::context
        Boost::system
        Boost::thread
        ${XXHASH_LIBRARY}
)

if (DISABLE_CHECK)
    message(STATUS "Disable runtime check macros")
    target_compile_definitions(idlekv_core
        PUBLIC
            DISABLE_CHECK=1
    )
endif()

option(ENABLE_ASIO_IO_URING "Enable Asio io_uring backend" OFF)

if (ENABLE_ASIO_IO_URING)
    find_library(LIBURING_LIBRARY uring)
    if (NOT LIBURING_LIBRARY)
        message(FATAL_ERROR
            "ENABLE_ASIO_IO_URING=ON but liburing was not found. "
            "Install liburing-dev, or configure with -DENABLE_ASIO_IO_URING=OFF.")
    endif()

    target_compile_definitions(idlekv_core
        PUBLIC
            BOOST_ASIO_HAS_IO_URING=1
            BOOST_ASIO_DISABLE_EPOLL=1
            BOOST_ASIO_HAS_IO_URING_AS_DEFAULT=1
    )

    target_link_libraries(idlekv_core
        PUBLIC
            ${LIBURING_LIBRARY}
    )
else()
    target_compile_definitions(idlekv_core
        PUBLIC
            BOOST_ASIO_DISABLE_IO_URING=1
    )
endif()

add_executable(idlekv "${CMAKE_CURRENT_SOURCE_DIR}/main.cc")

target_link_libraries(idlekv
    PRIVATE
        idlekv_core
)
