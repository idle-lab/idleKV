#pragma once

#include "common/asio_no_exceptions.h"
#include "common/config.h"
#include "common/logger.h"
#include "common/result.h"
#include "db/storage/data_entity.h"

#include <array>
#include <asio/asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/detail/is_buffer_sequence.hpp>
#include <asio/registered_buffer.hpp>
#include <asiochan/asiochan.hpp>
#include <charconv>
#include <chrono>
#include <climits>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace idlekv {

class DataEntity;

constexpr size_t kDefaultReadBufferSize = 4096;
constexpr size_t kDefaultWriteBufferSize = 2048;
constexpr size_t kMaxReplyFlushCount    = IOV_MAX - 2;
constexpr size_t kMaxReplyFlushBytes    = 64 * KB;
using byte                              = char;

constexpr const char* CRLF                 = "\r\n";
constexpr const char* SIMPLE_STRING_PREFIX = "+";
constexpr const char* ERROR_PREFIX         = "-";
constexpr const char* INTEGER_PREFIX       = ":";
constexpr const char* BULK_STRING_PREFIX   = "$";
constexpr const char* ARRAY_PREFIX         = "*";

enum class DataType : char {
    String     = '+',
    Error      = '-',
    Integers   = ':',
    BulkString = '$',
    Arrays     = '*'
};

auto operator==(DataType dt, char prefix) -> bool;

class Buf {
public:
    Buf() = default;
    Buf(byte* data, size_t size) : data_(data), size_(size) {}

    Buf(Buf&&)                         = default;
    Buf(const Buf&)                    = default;
    auto operator=(Buf&&) -> Buf&      = default;
    auto operator=(const Buf&) -> Buf& = default;

    auto Begin() -> byte* { return data_; }
    auto End() -> byte* { return data_ + size_; }
    auto Data() -> byte* { return const_cast<byte*>(data_); }
    auto Size() const -> size_t { return size_; }
    operator asio::mutable_buffer() const noexcept { return asio::mutable_buffer(data_, size_); }
    operator asio::const_buffer() const noexcept { return asio::const_buffer(data_, size_); }

private:
    byte* data_;
    size_t      size_;  
};

class BufView {
public:
    BufView() = default;
    BufView(const byte* data, size_t size) : data_(data), size_(size) {}

    BufView(BufView&&)                         = default;
    BufView(const BufView&)                    = default;
    auto operator=(BufView&&) -> BufView&      = default;
    auto operator=(const BufView&) -> BufView& = default;

    auto Begin() const -> const byte* { return data_; }
    auto End() const -> const byte* { return data_ + size_; }
    auto Data() -> byte* { return const_cast<byte*>(data_); }
    auto Data() const -> const byte* { return data_; }
    auto Size() const -> size_t { return size_; }
    operator asio::const_buffer() const noexcept { return asio::const_buffer(data_, size_); }

private:
    const byte* data_;
    size_t      size_;
};

class IOBuf {
public:
    IOBuf(size_t cap) : owner_(true) { Reserve(cap); }
    IOBuf(byte* data, size_t size) : buf_(data), cap_(size), owner_(false) {}
    IOBuf(const IOBuf&)                    = delete;
    auto operator=(const IOBuf&) -> IOBuf& = delete;

    IOBuf(IOBuf&& other) noexcept { MoveFrom(std::move(other)); }
    auto operator=(IOBuf&& other) noexcept -> IOBuf& {
        if (this != &other) {
            ReleaseOwnedBuffer();
            MoveFrom(std::move(other));
        }
        return *this;
    }

    ~IOBuf() { ReleaseOwnedBuffer(); }

    auto Defrag() -> void {
        if (r_ == 0) {
            return;
        }

        if (w_ > r_) {
            memmove(buf_, buf_ + r_, w_ - r_);
            w_ -= r_;
            r_ = 0;
        } else {
            w_ = r_ = 0;
        }
    }

    auto WriteView() -> BufView { return BufView{buf_ + w_, cap_ - w_}; }
    auto WriteSize() const -> size_t { return cap_ - w_; }
    auto WriteOffset() const -> size_t { return w_; }
    auto Commit(size_t n) -> void { w_ += n; }

    auto ReadView() -> BufView { return BufView{buf_ + r_, w_ - r_}; }
    auto Buffered() const -> size_t { return w_ - r_; }
    auto Consume(size_t n) -> void { r_ += n; }

    auto Capacity() const -> size_t { return cap_; }
    auto Empty() const -> bool { return r_ == w_; }
    auto Clear() -> void {
        r_ = 0;
        w_ = 0;
    }

    auto Data() -> byte* { return buf_; }
    auto Data() const -> const byte* { return buf_; }

    auto Reserve(size_t sz) -> void {
        CHECK(owner_);
        if (sz <= cap_)
            return;

        sz       = std::bit_ceil(sz);
        byte* nb = new (std::align_val_t{alignment_}) byte[sz];
        if (buf_) {
            if (w_ > r_) {
                memcpy(nb, buf_ + r_, w_ - r_);
                w_ -= r_;
                r_ = 0;
            } else {
                w_ = r_ = 0;
            }
            ::operator delete[](buf_, std::align_val_t{alignment_});
        }

        buf_         = nb;
        cap_         = sz;
    }

private:
    auto ReleaseOwnedBuffer() -> void {
        if (owner_ && buf_) {
            ::operator delete[](buf_, std::align_val_t{alignment_});
        }

        buf_         = nullptr;
        cap_         = 0;
        r_           = 0;
        w_           = 0;
    }

    auto MoveFrom(IOBuf&& other) -> void {
        cap_         = std::exchange(other.cap_, 0);
        buf_         = std::exchange(other.buf_, nullptr);
        alignment_   = other.alignment_;
        r_           = std::exchange(other.r_, 0);
        w_           = std::exchange(other.w_, 0);
        owner_ = std::exchange(other.owner_, false);
    }

    byte*  buf_{nullptr};
    size_t cap_{0};
    size_t alignment_{8};
    size_t r_{0}, w_{0};
    bool owner_;
};

class Reader {
public:
    Reader(size_t cap) : buf_(cap) {}
    Reader(byte* data, size_t size) : buf_(data, size) {}
    Reader(asio::mutable_registered_buffer buf) : buf_(static_cast<byte*>(buf.data()), buf.size()), reg_buf_(buf) {}

    auto ReadLineView() noexcept -> asio::awaitable<ResultT<std::string_view>>;
    auto ReadBytesTo(byte* buf, size_t len) noexcept -> asio::awaitable<ResultT<std::monostate>>;

    auto Fill() -> asio::awaitable<std::error_code>;
    auto HasMore() -> bool;
    auto Clear() -> void { buf_.Clear(); }

    virtual ~Reader() = default;
protected:
    virtual auto ReadImpl(byte* buf, size_t size) noexcept -> asio::awaitable<ResultT<size_t>> = 0;
    virtual auto ReadImpl(asio::mutable_registered_buffer reg_buf) noexcept -> asio::awaitable<ResultT<size_t>> = 0;
    virtual auto ReadvImpl(const std::vector<Buf>& bufs) noexcept -> asio::awaitable<ResultT<size_t>> = 0;

private:
    // buf_ and reg_buf_ refer to the same underlying memory region.
    // When we want to use io_uring registered buffers, reads should go through
    // reg_buf_, while buf_ must remain consistent throughout the process.
    IOBuf buf_;
    asio::mutable_registered_buffer reg_buf_;
    std::vector<Buf> bufs_;
};

class Writer {
public:
    Writer(size_t cap) : buf_(cap) {}

    // Copy small pieces into the internal buffer and append a single owned
    // slice to vecs_. When the current buffer cannot fit the whole packet,
    // this coroutine flushes pending output before growing the buffer.
    template <typename... Ts>
    auto WritePieces(Ts&&... pieces) -> asio::awaitable<std::error_code>;

    // caller should ensure that s is valid until the next flush.
    auto WriteView(std::string_view s) -> asio::awaitable<std::error_code>;

    // caller should ensure that s is valid until the next flush.
    auto Write(std::string_view s) -> asio::awaitable<std::error_code>;

    // Queue an external slice without copying and keep `holder` alive until the
    // pending reply batch is flushed.
    auto WriteRef(std::string_view s, std::shared_ptr<const void> holder)
        -> asio::awaitable<std::error_code>;

    // Flush all queued slices, including both owned buffer slices and
    // externally referenced views added via write().
    auto Flush() -> asio::awaitable<std::error_code>;

    auto Clear() -> void { ResetWriteState(); }

    virtual ~Writer() = default;
protected:
    virtual auto WriteImpl(const byte* data, size_t size) noexcept
        -> asio::awaitable<ResultT<size_t>> = 0;
    virtual auto WritevImpl(const std::vector<BufView>& bufs) noexcept
        -> asio::awaitable<ResultT<size_t>> = 0;

private:
    auto ResetWriteState() -> void;
    // Grow the owned buffer on demand. The new capacity is at least 2x the
    // current one, but never exceeds kMaxBufferSize.
    // auto reserve_buf(size_t required_piece_size) -> std::error_code;
    auto WritePieceSize(std::string_view piece) const -> size_t;

    template <typename T>
        requires(std::integral<std::remove_cvref_t<T>> &&
                 !std::same_as<std::remove_cvref_t<T>, bool>)
    auto WritePieceSize(T value) const -> size_t;

    auto WritePiece(byte*& out, std::string_view piece) -> void;

    template <typename T>
        requires(std::integral<std::remove_cvref_t<T>> &&
                 !std::same_as<std::remove_cvref_t<T>, bool>)
    auto WritePiece(byte*& out, T value) -> void;

    IOBuf buf_;
    std::vector<BufView> vecs_;
    std::vector<std::shared_ptr<const void>> keepalive_;
    size_t queued_size_{0};
    std::chrono::high_resolution_clock::time_point last_ = std::chrono::high_resolution_clock::now();
};

template <typename... Ts>
auto Writer::WritePieces(Ts&&... pieces) -> asio::awaitable<std::error_code> {
    const size_t total_size =
        (size_t{0} + ... + WritePieceSize(std::forward<Ts>(pieces)));

    if (total_size == 0) {
        co_return std::error_code{};
    }

    CHECK_LT(total_size, kDefaultWriteBufferSize);

    if (buf_.WriteSize() < total_size || queued_size_ + total_size >= kMaxReplyFlushBytes) {
        auto ec = co_await Flush();
        if (ec) {
            co_return ec;
        }
    }

    const size_t offset = buf_.Buffered();
    byte* begin = buf_.Data() + offset;
    byte* out   = begin;
    (WritePiece(out, std::forward<Ts>(pieces)), ...);

    queued_size_ += total_size;
    buf_.Commit(total_size);
    vecs_.emplace_back(begin, total_size);
    co_return std::error_code{};
}

inline auto Writer::WritePieceSize(std::string_view piece) const -> size_t { return piece.size(); }

template <typename T>
    requires(std::integral<std::remove_cvref_t<T>> &&
             !std::same_as<std::remove_cvref_t<T>, bool>)
auto Writer::WritePieceSize(T value) const -> size_t {
    using Value = std::remove_cvref_t<T>;

    char tmp[std::numeric_limits<Value>::digits10 + 3];
    auto [ptr, ec] = std::to_chars(tmp, tmp + sizeof(tmp), value);
    (void)ec;
    return static_cast<size_t>(ptr - tmp);
}

inline auto Writer::WritePiece(byte*& out, std::string_view piece) -> void {
    if (!piece.empty()) {
        std::memcpy(out, piece.data(), piece.size());
        out += piece.size();
    }
}

template <typename T>
    requires(std::integral<std::remove_cvref_t<T>> &&
             !std::same_as<std::remove_cvref_t<T>, bool>)
auto Writer::WritePiece(byte*& out, T value) -> void {
    using Value = std::remove_cvref_t<T>;

    char tmp[std::numeric_limits<Value>::digits10 + 3];
    auto [ptr, ec] = std::to_chars(tmp, tmp + sizeof(tmp), value);
    (void)ec;

    const size_t size = static_cast<size_t>(ptr - tmp);
    std::memcpy(out, tmp, size);
    out += size;
}

class ParserResut {
public:
    enum Status : uint8_t {
        OK,
        HAS_MORE,
        STD_ERROR,
        WRONG_TYPE_ERROR,
        PROTOCOL_ERROR,
    };

    ParserResut(Status s, std::vector<std::string>&& args)
        : s_(s), args_(std::move(args)) {}
    ParserResut(Status s, std::string&& msg)
        : s_(s), err_msg_(std::move(msg)) {}
    ParserResut(std::error_code ec)
        : s_(Status::STD_ERROR), err_msg_(ec.message()), ec_(ec) {}

    auto Ok() const -> bool { return s_ == Status::OK || s_ == Status::HAS_MORE; }

    auto Value() -> std::vector<std::string>& { return args_; }

    auto Message() const -> const std::string& { return err_msg_; }

    auto ErrorCode() const -> std::error_code { return ec_; }

    auto operator==(Status s) const -> bool { return s == s_; }

private:
    Status                   s_;
    std::string              err_msg_;
    std::error_code          ec_;
    std::vector<std::string> args_;
};

class Parser {
public:
    Parser(Reader* rd) : rd_(rd) {}

    // parse a Redis command
    auto ParseOne(std::vector<std::string>& args) noexcept -> asio::awaitable<ParserResut>;

    auto Clear() -> void { rd_->Clear(); }

private:
    Reader* rd_;
};

class Sender {
public:
    Sender(Writer* wr) : wr_(wr) {}

    auto SendSimpleString(std::string_view s) -> asio::awaitable<void>;
    auto SendOk() -> asio::awaitable<void>;
    auto SendPong() -> asio::awaitable<void>;
    auto SendBulkString(std::string_view s) -> asio::awaitable<void>;
    auto SendBulkString(const std::shared_ptr<const DataEntity>& data) -> asio::awaitable<void>;
    auto SendNullBulkString() -> asio::awaitable<void>;
    auto SendInteger(int64_t value) -> asio::awaitable<void>;
    auto SendError(std::string_view s) -> asio::awaitable<void>;

    auto Flush() -> asio::awaitable<void>;

    auto GetError() const -> std::error_code { return ec_; }

    auto Clear() -> void {
        ec_                            = std::error_code{};
        wr_->Clear();
    }

private:
    std::error_code ec_;

    Writer* wr_;
};

} // namespace idlekv
