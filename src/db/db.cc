#include "db/db.h"

#include "redis/protocol/reply.h"

#include <algorithm>
#include <cctype>
#include <string>
#include <utility>

namespace idlekv {

namespace {

auto to_lower(std::string s) -> std::string {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return s;
}

} // namespace

auto DB::exec(std::shared_ptr<Connection> /* conn */, const std::vector<std::string>& args) noexcept
    -> asio::awaitable<std::pair<std::string, std::unique_ptr<Err>>> {
    if (args.empty()) {
        co_return std::make_pair("", std::make_unique<SyntaxErr>());
    }

    const std::string cmd = to_lower(args[0]);

    if (cmd == "ping") {
        if (args.size() > 2) {
            co_return std::make_pair("", std::make_unique<ArgNumErr>("ping"));
        }

        if (args.size() == 2) {
            co_return std::make_pair(BulkString::make_reply(args[1], args[1].size()), nullptr);
        }

        co_return std::make_pair(SimpleString::make_reply("PONG"), nullptr);
    }

    if (cmd == "echo") {
        if (args.size() != 2) {
            co_return std::make_pair("", std::make_unique<ArgNumErr>("echo"));
        }
        co_return std::make_pair(BulkString::make_reply(args[1], args[1].size()), nullptr);
    }

    if (cmd == "set") {
        if (args.size() != 3) {
            co_return std::make_pair("", std::make_unique<ArgNumErr>("set"));
        }

        {
            std::lock_guard<std::mutex> lk(data_mu_);
            kv_[args[1]] = args[2];
        }

        co_return std::make_pair(SimpleString("OK").to_bytes(), nullptr);
    }

    if (cmd == "get") {
        if (args.size() != 2) {
            co_return std::make_pair("", std::make_unique<ArgNumErr>("get"));
        }

        std::string value;
        {
            std::lock_guard<std::mutex> lk(data_mu_);
            auto                        it = kv_.find(args[1]);
            if (it == kv_.end()) {
                co_return std::make_pair("$-1\r\n", nullptr);
            }
            value = it->second;
        }

        co_return std::make_pair(BulkString::make_reply(value, value.size()), nullptr);
    }

    if (cmd == "del") {
        if (args.size() < 2) {
            co_return std::make_pair("", std::make_unique<ArgNumErr>("del"));
        }

        uint64_t removed = 0;
        {
            std::lock_guard<std::mutex> lk(data_mu_);
            for (size_t i = 1; i < args.size(); ++i) {
                removed += static_cast<uint64_t>(kv_.erase(args[i]));
            }
        }

        co_return std::make_pair(Integer::make_reply(removed), nullptr);
    }

    co_return std::make_pair("", std::make_unique<UnknownCmdErr>(args[0]));
}

} // namespace idlekv
