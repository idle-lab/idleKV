#include <redis/type/base.h>

namespace idlekv {

const char* CRLF = "\r\n";


auto Decoder::read_line() -> asio::awaitable<std::string> {
    std::string line;

    // 如果缓冲区中没有完整的一行数据，则直接从 in_ 中读取，直到读完一整行
    if (size_t len = buffer_find('\n'); len != std::string::npos) {
        line = buffer_get(len);
    } else {
        line = std::move(buffer_);
        r    = 0;

        while (true) {
            auto [chunk, done] = co_await in_.read();
            if (done) {
                throw std::runtime_error("Connection closed");
            }

            if (size_t p = chunk.find('\n'); p != std::string::npos) {
                buffer_ = chunk.substr(p + 1);
                line += chunk.substr(0, p + 1);

                break;
            } else {
                line += chunk;
            }
        }
    }

    co_return line;
}

auto Decoder::read_bytes(size_t len) -> asio::awaitable<std::string> {
    while (buffer_size() < len) {
        co_await buffer_fill();
    }

    co_return buffer_get(len);
}

} // namespace idlekv
