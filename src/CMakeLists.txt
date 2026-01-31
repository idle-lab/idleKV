set(SOURCES
    main.cc
    # 索引层
    index/art.cc
    server/server.cc
    server/config.cc

    redis/handler.cc

    common/logger.cc

    utils/timer/timer.hpp

    utils/io/writer.hpp
)

add_executable(idlekv ${SOURCES})

target_include_directories(idlekv
    PRIVATE
        ${PROJECT_DIR}/src
)

target_link_libraries(idlekv
    PRIVATE
        third_party
)

target_compile_definitions(idlekv
    PRIVATE
        ASIOCHAN_USE_STANDALONE_ASIO=ON
)