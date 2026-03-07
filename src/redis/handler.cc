#include "redis/handler.h"

#include "common/logger.h"
#include "db/context.h"
#include "redis/protocol/error.h"
#include "redis/protocol/parser.h"
#include "server/thread_state.h"

#include <asio/as_tuple.hpp>
#include <asio/asio/error.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
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

} // namespace

asio::awaitable<void> RespHandler::handle(asio::ip::tcp::socket socket) {
    auto conn = std::make_shared<Connection>(std::move(socket));
    LOG(debug, "connect a new client, {}:{}", conn->remote_endpoint().address().to_string(),
        conn->remote_endpoint().port());
    auto& io = ThreadState::tlocal()->io_context();
    auto ctx = Context(conn, io);
    auto& p = ctx.parser();
    auto& s= ctx.sender();

    for (;;) {
        auto res = co_await p.parse_one();
        if (!res.ok()) {
            if (res == ParserResut::STD_ERROR) {
                if (is_connection_closed_error(res.error_code())) {
                    break;
                }

                if (is_transient_io_error(res.error_code())) {
                    continue;
                }

                break;
            }

            // Protocol-level parse errors make stream state unreliable; close after reply.
            break;
        }

        co_await s.send(engine_->exec(ctx, res.value()));

        if (res != ParserResut::HAS_MORE) {
            co_await s.flush();
        }
    }

    conn->close();
}

} // namespace idlekv
