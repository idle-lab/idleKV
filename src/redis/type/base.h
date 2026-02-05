#pragma once

#include <string>
#include <asio/asio.hpp>
#include <asiochan/asiochan.hpp>

namespace idlekv {

enum class DataType : char { 
	String      = '+', 
	Error       = '-',
    Integers    = ':',
    BulkString  = '$',
    Arrays      = '*'
};

const char* CRLF = "\r\n";

struct Payload {
    std::string msg;
    bool        done;

    Payload(std::string&& m, bool d) : msg(std::move(m)), done(d) {}

    Payload(std::string& m, bool d) : msg(std::move(m)), done(d) {}
};

class Encoder {

};

class Decoder {
public:
    Decoder(asiochan::read_channel<Payload> in) : in_(in) {}

    asio::awaitable<std::string> read_line() {
        size_t pos = 0;

        while ((buffer_.find("\r\n", pos)) == std::string::npos) {
            pos = buffer_.size();
            co_await do_read();
        }

        auto line = buffer_.substr(0, pos);
        buffer_          = buffer_.substr(pos + 2);
        co_return line;
    }

    asio::awaitable<std::string> read_bytes(size_t len) { 
        while (buffer_.size() < len) {
            co_await do_read();
        }

        auto data = buffer_.substr(0, len);
        buffer_   = buffer_.substr(len);
        co_return data;
    }

    asio::awaitable<int> read_int() {

    }

    asio::awaitable<int> read_uint() {
    
    }

    asio::awaitable<char> read_byte() { 
        if (buffer_.size() < 1) {

        }
    }

private:
    asio::awaitable<char> do_read() {
        auto [chunk, done] = co_await in_.read();
        if (done) {
            throw std::runtime_error("Connection closed before line was complete");
        }
        buffer_ += chunk;
    }

    size_t rp = 0, wp = 0;
    std::string buffer_;
    asiochan::read_channel<Payload> in_;
};




} // namespace idlekv
