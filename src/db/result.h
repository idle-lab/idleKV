#pragma once

#include "db/storage/kvstore.h"
#include "utils/condition_variable/condition_variable.h"
#include <atomic>
#include <asio/any_io_executor.hpp>
#include <asio/awaitable.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace idlekv {

class ExecResult {
public:
    enum Type : uint8_t {
        kPong,
        kOk,
        kSimpleString,
        kBulkString,
        kNull,
        kInteger,
        kError,
    };

    ExecResult() = default;

    static auto pong() -> ExecResult { return ExecResult(kPong); }
    static auto ok() -> ExecResult { return ExecResult(kOk); }

    static auto simple_string(std::string value) -> ExecResult {
        return ExecResult(kSimpleString, std::move(value));
    }

    static auto bulk_string(std::string value) -> ExecResult {
        return ExecResult(kBulkString, std::move(value));
    }

    static auto bulk_string(const std::shared_ptr<DataEntity>& data) -> ExecResult {
        return ExecResult(kBulkString, data);
    }


    static auto bulk_string(std::string_view value) -> ExecResult {
        return ExecResult(kBulkString, std::string(value));
    }

    static auto null() -> ExecResult { return ExecResult(kNull); }

    static auto integer(int64_t value) -> ExecResult { return ExecResult(kInteger, value); }

    static auto error(std::string value) -> ExecResult {
        return ExecResult(kError, std::move(value));
    }

    auto type() const -> Type { return type_; }

    auto is_ok() const -> bool { return type_ == kOk; }

    auto string() const -> std::string_view { return string_; }

    auto data() -> const std::shared_ptr<DataEntity>& { return data_; }
    auto data() const -> const std::shared_ptr<DataEntity>& { return data_; }

    auto integer() const -> int64_t { return integer_; }

private:
    explicit ExecResult(Type type) : type_(type) {}

    ExecResult(Type type, std::string value) : type_(type), string_(std::move(value)) {}
    ExecResult(Type type, const std::shared_ptr<DataEntity>& data) : type_(type), data_(data) {}

    ExecResult(Type type, int64_t value) : type_(type), integer_(value) {}

    Type        type_ = kOk;
    std::string string_;
    std::shared_ptr<DataEntity> data_;
    int64_t     integer_ = 0;
};

class PromiseResult {
public:
    using clock = std::chrono::steady_clock;

    PromiseResult(const asio::any_io_executor& executor) : cv_(executor) {}

    auto notify() -> void {
        ready_.store(true, std::memory_order_release);
        cv_.notify();
    }
    auto async_wait() -> asio::awaitable<void> {
        if (ready_.load(std::memory_order_acquire)) {
            co_return;
        }

        co_await cv_.async_wait();
    }

    auto set_reslute(ExecResult res) -> void {
        res_ = std::move(res);
        ready_.store(false, std::memory_order_release);
    }

    auto mark_shard_enqueued(clock::time_point now = clock::now()) -> void {
        stage_tracking_   = true;
        shard_enqueued_at_ = now;
    }

    auto mark_send_ready(clock::time_point now = clock::now()) -> void {
        if (!stage_tracking_) {
            return;
        }
        send_ready_at_ = now;
    }

    auto has_stage_tracking() const -> bool { return stage_tracking_; }
    auto shard_enqueued_at() const -> clock::time_point { return shard_enqueued_at_; }
    auto send_ready_at() const -> clock::time_point { return send_ready_at_; }

    auto result() -> ExecResult& { return res_; }
    auto get_executor() -> const asio::any_io_executor& { return cv_.get_executor(); }
private:
    ExecResult res_;
    std::atomic<bool> ready_{false};
    bool stage_tracking_{false};
    clock::time_point shard_enqueued_at_{};
    clock::time_point send_ready_at_{};
    utils::DisposableConditionVariable cv_;
};

} // namespace idlekv
