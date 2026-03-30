#include "redis/parser.h"

#include "common/logger.h"
#include "common/result.h"
#include "metric/prometheus.h"

#include <algorithm>
#include <boost/asio/buffer_registration.hpp>
#include <charconv>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace idlekv {

auto operator==(DataType dt, char prefix) -> bool { return static_cast<char>(dt) == prefix; };

namespace {

// debug function
[[maybe_unused]] std::string EscapeString(std::string_view s) {
    std::string out;
    out.reserve(s.size() * 2);

    for (unsigned char c : s) {
        switch (c) {
        case '\n':
            out += "\\n";
            break;
        case '\r':
            out += "\\r";
            break;
        case '\t':
            out += "\\t";
            break;
        case '\0':
            out += "\\0";
            break;
        case '\\':
            out += "\\\\";
            break;
        case '\"':
            out += "\\\"";
            break;
        default:
            if (std::isprint(c)) {
                out += c;
            } else {
                char buf[5];
                std::snprintf(buf, sizeof(buf), "\\x%02X", c);
                out += buf;
            }
        }
    }

    return out;
}

template <typename T>
    requires(std::integral<std::remove_cvref_t<T>> && !std::same_as<std::remove_cvref_t<T>, bool>)
auto DecimalSize(T value) -> size_t {
    using Value = std::remove_cvref_t<T>;

    char tmp[std::numeric_limits<Value>::digits10 + 3];
    auto [ptr, ec] = std::to_chars(tmp, tmp + sizeof(tmp), value);
    (void)ec;
    return static_cast<size_t>(ptr - tmp);
}

} // namespace

auto Reader::ReadLineView() noexcept -> ResultT<std::string_view> {
    for (;;) {
        auto rv = buf_.ReadView();
        // auto pos = static_cast<const byte*>(std::memchr(rv.Data(), '\n', rv.Size()));
        auto pos = std::find(rv.Begin(), rv.End(), '\n');
        if (pos == rv.End()) {
            CHECK(rv.Size() < buf_.Capacity()) << "line length exceeds buffer size";
            buf_.Defrag();
            auto ec = Fill();
            if (ec) {
                return ec;
            }
            continue;
        }

        std::string_view line(rv.Data(), pos - rv.Data() + 1);
        buf_.Consume(pos + 1 - rv.Data());
        return line;
    }
}

auto Reader::ReadBytesTo(char* buf, size_t len) noexcept -> std::error_code {
    size_t offset = 0;

    auto rv = buf_.ReadView();
    if (rv.Size() >= len) {
        std::memcpy(buf + offset, rv.Data(), len);
        buf_.Consume(len);
        return std::error_code{};
    }

    std::memcpy(buf + offset, rv.Data(), rv.Size());
    offset += rv.Size();
    len -= rv.Size();

    buf_.Clear();

    bufs_[0] = Buf{buf + offset, len};
    auto wv  = buf_.WriteView();
    bufs_[1] = Buf{wv.Data(), wv.Size()};

    for (;;) {
        auto res = ReadvImpl(bufs_);
        if (!res.Ok()) {
            return res.Err();
        }
        if (res.Value() < len) {
            offset += res.Value();
            len -= res.Value();
            bufs_[0] = Buf{buf + offset, len};
        } else {
            buf_.Commit(res.Value() - len);
            return std::error_code{};
        }
    }
}

auto Reader::SkipCRLF() noexcept -> std::error_code {
    size_t n = 2;
    while (buf_.Buffered() < n) {
        n -= buf_.Buffered();
        buf_.Consume(buf_.Buffered());
        buf_.Clear();
        auto ec = Fill();
        if (ec) {
            return ec;
        }
    }

    buf_.Consume(n);
    return std::error_code{};
}


auto Reader::Fill() -> std::error_code {
    ResultT<size_t> res{std::error_code{}};
    if (reg_buf_.data()) {
        auto reg_wv = asio::buffer(reg_buf_ + buf_.WriteOffset(), buf_.WriteSize());
        res         = ReadImpl(reg_wv);
    } else {
        auto wv = buf_.WriteView();
        res     = ReadImpl(wv.Data(), wv.Size());
    }

    if (!res.Ok()) {
        return res.Err();
    }
    buf_.Commit(res.Value());
    return std::error_code{};
}

auto Reader::HasMore() -> bool { return buf_.Buffered() > 0; }

auto Writer::ResetWriteState() -> void {
    buf_.Clear();
    vecs_.clear();
    keepalive_.clear();
    queued_size_ = 0;
}

auto Writer::WriteView(std::string_view s) -> std::error_code {
    if (s.empty()) {
        return std::error_code{};
    }

    if (!vecs_.empty() &&
        (vecs_.size() >= kMaxReplyFlushCount || queued_size_ + s.size() >= kMaxReplyFlushBytes)) {
        auto ec = Flush();
        if (ec) {
            return ec;
        }
    }

    queued_size_ += s.size();
    vecs_.emplace_back(s.data(), s.size());

    return std::error_code{};
}

auto Writer::Write(std::string_view s) -> std::error_code {
    if (buf_.WriteSize() < s.size() || queued_size_ + s.size() >= kMaxReplyFlushBytes) {
        auto ec = Flush();
        if (ec) {
            return ec;
        }
    }

    if (buf_.WriteSize() < s.size()) {
        buf_.Reserve(buf_.Buffered() + s.size());
    }

    const size_t offset = buf_.Buffered();
    char*        begin  = buf_.Data() + offset;
    std::memcpy(begin, s.data(), s.size());

    queued_size_ += s.size();
    buf_.Commit(s.size());
    vecs_.emplace_back(begin, s.size());

    return std::error_code{};
}

auto Writer::WriteRef(std::string_view s, std::shared_ptr<const void> holder) -> std::error_code {
    if (s.empty()) {
        return std::error_code{};
    }

    if (!vecs_.empty() &&
        (vecs_.size() >= kMaxReplyFlushCount || queued_size_ + s.size() >= kMaxReplyFlushBytes)) {
        auto ec = Flush();
        if (ec) {
            return ec;
        }
    }

    queued_size_ += s.size();
    vecs_.emplace_back(s.data(), s.size());
    if (holder) {
        keepalive_.push_back(std::move(holder));
    }

    return std::error_code{};
}

auto Writer::Flush() -> std::error_code {
    if (vecs_.empty()) {
        return std::error_code{};
    }

    // vecs_ preserves the original enqueue order, so a single writev keeps
    // mixed owned/external pieces in the same packet order they were queued.
    auto res = WritevImpl(vecs_);
    ResetWriteState();
    return res.Err();
}

auto Parser::ParseOne(CmdArgs& args) noexcept -> ParserResut {
    auto headerRes = rd_->ReadLineView();
    if (!headerRes.Ok()) {
        return headerRes.Err();
    }

    auto& header = headerRes.Value();
    if (header[0] != DataType::Arrays) [[unlikely]] {
        return ParserResut(ParserResut::PROTOCOL_ERROR,
                           fmt::format("need '*' but '{}'", header[0]));
    }
    int arrLen;
    auto [ptr, err] = std::from_chars(header.data() + 1, header.data() + header.size() - 2, arrLen);
    if (err != std::errc()) [[unlikely]] {
        return std::make_error_code(err);
    }

    for (int i = 0; i < arrLen; ++i) {
        auto lineRes = rd_->ReadLineView();
        if (!lineRes.Ok()) {
            return lineRes.Err();
        }

        auto& line = lineRes.Value();
        if (line.size() < 4 || line[0] != DataType::BulkString) [[unlikely]] {
            return ParserResut(ParserResut::PROTOCOL_ERROR,
                               fmt::format("need $ but '{}'", line[0]));
        }

        int strLen;
        auto [ptr, err] = std::from_chars(line.data() + 1,
                                          line.data() + line.size() - 2 /* exclude CRLF */, strLen);
        if (err != std::errc()) [[unlikely]] {
            return std::make_error_code(err);
        }

        // empty bulk string
        if (strLen == -1) {
            continue;
        }

        args.PushArg(strLen);

        if (auto ec = rd_->ReadBytesTo(const_cast<char*>(args[i].data()), strLen); ec) {
            return ec;
        }

        // skip the trailing CRLF
        if (auto ec = rd_->SkipCRLF(); ec) {
            return ec;
        }
    }
    return ParserResut(rd_->HasMore() ? ParserResut::HAS_MORE : ParserResut::OK);
}

auto Sender::SendSimpleString(std::string_view s) -> void {
    BatchGuard bg(this);
    ec_ = wr_->WritePieces(SIMPLE_STRING_PREFIX, s, CRLF);
}

auto Sender::SendOk() -> void {
    BatchGuard bg(this);
    ec_ = wr_->WritePieces("+OK\r\n");
}

auto Sender::SendPong() -> void {
    BatchGuard bg(this);
    ec_ = wr_->WritePieces("+PONG\r\n");
}

auto Sender::SendBulkString(std::string_view s) -> void {
    BatchGuard bg(this);
    ec_ = wr_->WritePieces(BULK_STRING_PREFIX, s.size(), CRLF);
    if (!ec_) {
        ec_ = wr_->WriteView(s);
    }
    if (!ec_) {
        ec_ = wr_->WritePieces(CRLF);
    }
}

auto Sender::SendBulkString(const std::shared_ptr<const DataEntity>& data) -> void {
    BatchGuard bg(this);
    if (!data) {
        SendNullBulkString();
        return;
    }

    const auto& value = data->AsString();
    ec_               = wr_->WritePieces(BULK_STRING_PREFIX, value.size(), CRLF);
    if (!ec_) {
        ec_ = wr_->WriteRef(value, data);
    }
    if (!ec_) {
        ec_ = wr_->Write(CRLF);
    }
}

auto Sender::SendNullBulkString() -> void {
    BatchGuard bg(this);
    ec_ = wr_->WritePieces("$-1\r\n");
}

auto Sender::SendInteger(int64_t value) -> void {
    BatchGuard bg(this);
    ec_ = wr_->WritePieces(INTEGER_PREFIX, value, CRLF);
}

auto Sender::SendError(std::string_view s) -> void {
    BatchGuard bg(this);
    PrometheusMetrics::Instance().OnErrorResponse();
    ec_ = wr_->WritePieces(ERROR_PREFIX, s, CRLF);
}

auto Sender::Flush() -> void { ec_ = wr_->Flush(); }

} // namespace idlekv
