#pragma once

#include <asio/asio.hpp>

namespace idlekv {

class Writer {
public:
	Writer() = default;

	virtual size_t write(const char* data, size_t n) = 0;

	virtual ~Writer() = default;
};

class WithBuffer {
public:
    WithBuffer() = default;
	
	virtual void flush() = 0;

    virtual ~WithBuffer() = default;
};

class AsioWriter : public Writer {
public:
	explicit AsioWriter(asio::ip::tcp::socket&& socket) 
		: socket_(std::move(socket)) 
	{}

	virtual size_t write(const char* data, size_t n) override {
		return asio::write(socket_, asio::buffer(data, n));
	}
	
	virtual asio::awaitable<size_t> async_write(const char* data, size_t n) {
        return socket_.async_write_some(asio::buffer(data, n), asio::use_awaitable);
	}

    ~AsioWriter() override = default;

protected:
    asio::ip::tcp::socket socket_;
};

} // namespace idlekv
