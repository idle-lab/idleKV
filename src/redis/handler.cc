#include "redis/handler.h"
#include "common/logger.h"

#include "redis/protocol/error.h"
#include "redis/protocol/parser.h"

#include <asio/co_spawn.hpp>
#include <asio/error.hpp>
#include <asio/post.hpp>
#include <asio/use_awaitable.hpp>
#include <asiochan/asiochan.hpp>
#include <asiochan/select.hpp>
#include <memory>
#include <system_error>

namespace idlekv {

asio::awaitable<void> RESPHandler::handle(asio::ip::tcp::socket socket) {
    auto conn = std::make_shared<Connection>(std::move(socket));
    LOG(debug, "connect a new client, {}:{}", conn->remote_endpoint().address().to_string(), conn->remote_endpoint().port());
    for (;;) {
        Parser p(conn);

        auto [args, err] = co_await p.parse_one();
        if (err != nullptr) {
            if (err->is_standard_error()) {
                break;
            }

            auto ec = co_await conn->write(err->to_bytes());
            if (ec != std::error_code()) {
                break;
            }

            break;
        }

        co_await conn->write(SimpleString("OK").to_bytes());
    }

    conn->close();
}

// asio::awaitable<void> RedisHandler::parse_and_execute(asiochan::channel<Payload> in,
//                                                       asiochan::channel<Payload> out,
//                                                       asiochan::channel<void, 3> doneCh) {
//     try {
//         for (;;) {
//             Parser p{in};

//             auto cmd = co_await p.parse_one();
//         }
//     } catch (std::exception& e) {
//         LOG(error, "Parse and execute exception: {}", e.what());
//     }

//     co_await doneCh.write();
// }

} // namespace idlekv
