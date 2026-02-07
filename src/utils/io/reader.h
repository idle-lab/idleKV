#pragma once

#include <asio/asio.hpp>
#include <memory>

namespace idlekv {

class Reader {
public:
    Reader() = default;

    virtual size_t read(const char* data, size_t n) = 0;

    virtual ~Reader() = default;
};

class AsyncReader {
public:
    AsyncReader() = default;

    virtual asio::awaitable<size_t> async_read(char* data, size_t n) = 0;

    virtual ~AsyncReader() = default;
};

} // namespace idlekv
