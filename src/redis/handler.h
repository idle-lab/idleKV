#pragma once

#include <server/handler.h>
#include <server/server.h>
#include <common/config.h>
#include <redis/connection.h>
#include <vector>
#include <memory>
#include <cstdlib>
#include <string>
#include <asiochan/asiochan.hpp>

namespace idlekv {

struct Payload {
    std::string msg;
    bool        done;

    Payload(std::string&& m, bool d)
        : msg(std::move(m)), done(d) {}

    Payload(std::string& m, bool d)
        : msg(std::move(m)), done(d) {}
};

class RedisHandler : public Handler {
public:
    RedisHandler(const Config& cfg, std::shared_ptr<Server> srv) 
    : Handler(cfg.ip_, std::atoi(cfg.port_.c_str())), 
      srv_(srv)
    { }

    asio::awaitable<void> handle(asio::ip::tcp::socket socket);

    asio::awaitable<void> parse_and_execute(std::shared_ptr<Connection> conn, 
                                            asiochan::channel<Payload>  in,
                                            asiochan::channel<Payload>  out,
                                            asiochan::channel<void, 3>  doneCh);

    virtual asio::awaitable<void> listen() override;

    virtual void stop() override {}

    virtual std::string name() override { return "Redis"; }

private:
    std::vector<Connection> conns_;
    std::shared_ptr<Server> srv_;
};


class Encoder {
public:

private:
    asiochan::channel<Payload> out;
};


class Decoder {


private:
    asiochan::channel<Payload> in;
};

} // namespace idlekv
