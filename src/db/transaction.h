#pragma once

#include "db/command.h"

#include <cstddef>
#include <cstdint>
#include <vector>
#include <absl/functional/function_ref.h>

namespace idlekv {

using TxnId = uint64_t;

// The index position of the key in args
class Keys {};

class CommandContext {
public:
    Cmd* cmd;
    Keys read_set_, write_set_;
};

class MultiCmd {
public:
    auto Size() -> size_t { return cmds_.size(); }

private:
    std::vector<CommandContext> cmds_;
};

class Transaction {
public:
    using Task = absl::FunctionRef<void()>;

    auto Init() -> void {}

    auto Schedule(Task task) -> void {
        
    }

private:
    TxnId txn_id_;

    // The number of data shards involved in the transaction
    size_t shard_count_;

    MultiCmd* multi_;
};

} // namespace idlekv