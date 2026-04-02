#pragma once

#include "db/transaction.h"
#include <cstddef>
#include <cstdint>

namespace idlekv {

class Connection;

// Client represents a connected client, and tracks its state, such as current transaction, selected db, etc.
class Client {
public:
    explicit Client(Connection* connection) : conn(connection) {}

    enum struct ExecState : uint8_t {
        MultiInactive, // not in multi
        MultiCollect,   // in multi, collecting cmds
        MultiRunning,  // exec called, running cmds
    };

    Transaction* txn{nullptr};
    ExecState   exec_state{ExecState::MultiInactive};
    size_t db_index{0};
    Connection* conn;

    // TODO(cyb): Track memory usage of a client.
};

}