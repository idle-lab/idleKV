set(SOURCES
    main.cc
    # 索引层
    index/art.cc
    server/server.cc
    server/config.cc

    redis/handler.cc

    common/logger.cc
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