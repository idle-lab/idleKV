#pragma once

#include "common/logger.h"
#include "db/command.h"
#include "db/shard.h"
#include "redis/connection.h"
#include "redis/parser.h"
#include "utils/coroutine/generator.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <vector>
#include <absl/functional/function_ref.h>

namespace idlekv {

using TxnId = uint64_t;


enum class MultiMode : uint8_t {
    Default,
    Squash,
};

class MultiCmd {
public:
    explicit MultiCmd(MultiMode mode = MultiMode::Default) : mode_(mode) {}

    auto Size() -> size_t { return cmds_.size(); }

    auto Append(Cmd* cmd, CmdArgsPtr args, WRSet keys) -> void {
        cmds_.emplace_back(cmd, std::move(args), keys);
    }

    auto Mode() -> MultiMode { return mode_; }

    auto Next() -> utils::Generator<CommandContext> {
        for (auto& cmd : cmds_) {
            co_yield std::move(cmd);
            progress_++;
        }
    }

private:
    std::vector<CommandContext> cmds_;
    size_t progress_{0};
    MultiMode mode_;
};

class Transaction {
public:

    template<class Fn>
    auto Schedule(Fn&& task) -> std::invoke_result_t<Fn> {
        if (multi_ && multi_->Mode() == MultiMode::Squash) {
            return task();
        }

        using namespace boost::fibers;

        // TODO(cyb): support multi-shard transaction.
        CHECK(shards_.size() == 1);

        if (shards_[0]->GetShardId() == owner_id_) {
            return task();
        } else {
            auto prom = std::make_shared<promise<std::invoke_result_t<Fn>>>();
            auto fut  = prom->get_future();
            shards_[0]->Add([task = std::move(task), prom]() mutable {
                prom->set_value(task());
            });
            return fut.get();
        }
    }

private:
    TxnId txn_id_;

    std::vector<Shard*> shards_;

    std::unique_ptr<MultiCmd> multi_;

    CommandContext cmd_ctx_;

    SenderBase* sender_;
    Connection* conn_;
    uint8_t owner_id_;
};

} // namespace idlekv