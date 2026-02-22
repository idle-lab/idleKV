#include "redis/handler.h"
#include "common/logger.h"

#include "redis/protocol/error.h"
#include "redis/protocol/parser.h"

#include <asio/asio/error.hpp>
#include <asio/co_spawn.hpp>
#include <asio/error.hpp>
#include <asio/post.hpp>
#include <asio/use_awaitable.hpp>
#include <asiochan/asiochan.hpp>
#include <asiochan/select.hpp>
#include <memory>
#include <system_error>

namespace idlekv {

namespace {

auto is_connection_closed_error(const std::error_code& ec) -> bool {
    return ec == asio::error::eof || ec == asio::error::connection_reset ||
           ec == asio::error::connection_aborted || ec == asio::error::not_connected ||
           ec == asio::error::broken_pipe || ec == asio::error::bad_descriptor ||
           ec == asio::error::operation_aborted;
}

auto is_transient_io_error(const std::error_code& ec) -> bool {
    return ec == asio::error::try_again || ec == asio::error::would_block ||
           ec == asio::error::timed_out || ec == asio::error::interrupted;
}

auto is_fatal_write_error(const std::error_code& ec) -> bool {
    return is_connection_closed_error(ec) || !is_transient_io_error(ec);
}

auto try_write_err(std::shared_ptr<Connection> conn, std::unique_ptr<Err>& err)
    -> asio::awaitable<std::error_code> {
    if (err == nullptr) {
        co_return std::error_code();
    }

    co_return co_await conn->write(err->to_bytes());
}

} // namespace

asio::awaitable<void> RESPHandler::handle(asio::ip::tcp::socket socket) {
    auto conn = std::make_shared<Connection>(std::move(socket));
    LOG(debug, "connect a new client, {}:{}", conn->remote_endpoint().address().to_string(), conn->remote_endpoint().port());
    Parser p(conn);

    for (;;) {
        auto [args, err] = co_await p.parse_one();
        if (err != nullptr) {
            if (err->is_standard_error()) {
                auto* standard = dynamic_cast<StandardErr*>(err.get());
                if (standard != nullptr && is_connection_closed_error(standard->error_code())) {
                    break;
                }

                if (standard != nullptr && is_transient_io_error(standard->error_code())) {
                    continue;
                }

                break;
            }

            auto ec = co_await try_write_err(conn, err);
            if (ec != std::error_code() && is_fatal_write_error(ec)) {
                break;
            }

            // Protocol-level parse errors make stream state unreliable; close after reply.
            break;
        }

        auto [reply, exec_err] = co_await db_->exec(conn, args);
        if (exec_err != nullptr) {
            auto ec = co_await try_write_err(conn, exec_err);
            if (ec != std::error_code() && is_fatal_write_error(ec)) {
                break;
            }

            if (exec_err->is_standard_error()) {
                auto* standard = dynamic_cast<StandardErr*>(exec_err.get());
                if (standard != nullptr && is_connection_closed_error(standard->error_code())) {
                    break;
                }
            }

            continue;
        }

        auto ec = co_await conn->write(reply);
        if (ec != std::error_code()) {
            if (is_connection_closed_error(ec)) {
                break;
            }

            if (is_transient_io_error(ec)) {
                continue;
            }

            break;
        }
    }

    conn->close();
}

} // namespace idlekv
