set(SOURCES
    # 索引层
    db/index/art.cc
    server/server.cc
    server/config.cc

    redis/handler.cc
    redis/connection.cc
    redis/protocol/parser.cc

    common/logger.cc

    utils/timer/timer.cc
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

add_executable(idlekv main.cc)

target_link_libraries(idlekv
    PRIVATE
        idlekv_core
)


