#include "redis/handler.h"
#include "redis/command.h"

#include "redis/protocol/parser.h"

#include <asiochan/asiochan.hpp>
#include <asiochan/select.hpp>
#include <exception>

namespace idlekv {

asio::awaitable<void> RedisHandler::handle(asio::ip::tcp::socket socket) {
    auto conn = std::make_shared<Connection>(std::move(socket));

    try {
        Parser p(conn);

        auto c = Cmd(co_await p.parse_one());


    } catch (const std::exception& e) {
    }


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
