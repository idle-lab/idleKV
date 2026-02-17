#include "redis/handler.h"

#include "redis/protocol/parser.h"

#include <asio/co_spawn.hpp>
#include <asio/post.hpp>
#include <asio/use_awaitable.hpp>
#include <asiochan/asiochan.hpp>
#include <asiochan/select.hpp>

namespace idlekv {

asio::awaitable<void> RedisHandler::handle(asio::ip::tcp::socket socket) {
    auto conn = std::make_shared<Connection>(std::move(socket));

    try {
        Parser p(conn);

        auto [args, err] = co_await p.parse_one();
        if (err != nullptr) {
            
        }




        auto res = co_await asio::co_spawn(
            srv_->get_worker_pool(), []() -> asio::awaitable<std::string> { co_return "21"; }(),
            asio::use_awaitable);
        

    } catch (...) {
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
