#pragma once

#include "db/shard.h"
#include "db/transaction.h"
#include "redis/parser.h"
#include <cstddef>
#include <cstdint>
#include <memory>

namespace idlekv {

class Connection;

// Client represents a connected client, and tracks its state, such as current transaction, selected db, etc.
class ExecContext {
public:
    ExecContext() = default;
    explicit ExecContext(Connection* connection) : conn(connection) {}

    enum struct ExecState : uint8_t {
        MultiInactive, // not in multi
        MultiCollect,   // in multi, collecting cmds
        MultiRunning,  // exec called, running cmds
    };

    auto CurTxn() -> Transaction* {
        return txn.get();
    }

    std::unique_ptr<Transaction> txn{nullptr};
    ExecState   exec_state{ExecState::MultiInactive};
    size_t db_index{0};
    Connection* conn{nullptr};
    SenderBase* sender{nullptr};

    // TODO(cyb): Track memory usage of a client.
};

}
