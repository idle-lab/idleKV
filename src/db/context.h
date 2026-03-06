#pragma once

#include "db/db.h"
#include "redis/connection.h"
#include "redis/protocol/parser.h"
#include "redis/protocol/reply.h"
#include "server/thread_state.h"

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <cstddef>
#include <memory>
#include <queue>
#include <string_view>
#include <vector>
namespace idlekv {

// all contextual information required for command execution
class Context {
public:

    explicit Context(const std::shared_ptr<Connection>& conn, asio::io_context& io)
        : owner_(conn), parser_(conn.get()), sender_(conn.get()), sender_timer_(io) {}

    Context(const Context&)                    = delete;
    auto operator=(const Context&) -> Context& = delete;

    auto connection() const -> Connection* { return owner_.get(); }

    auto parser() -> Parser& { return parser_; }

    auto sender() -> Sender& { return sender_; }

    auto start_a_sender_timer() -> void { has_async_timer_ = true; }
    auto has_sender_timer() -> bool { return has_async_timer_; }
    auto sender_timer_done() -> void { has_async_timer_ = false; }
    auto sender_timer() -> asio::steady_timer& { return sender_timer_; }
private:
    std::shared_ptr<Connection> owner_;

    Parser parser_;

    Sender sender_;
    asio::steady_timer sender_timer_;
    bool has_async_timer_;
};

} // namespace idlekv