#pragma once

#include "db/shard.h"
#include "redis/client.h"
#include "redis/connection.h"
#include "redis/parser.h"

namespace idlekv {

struct ExecContext {
    auto CurTxn() -> Transaction* {
        CHECK(client->txn != nullptr);
        return client->txn;
    }

    Client*      client;
    SenderBase* sender;
    size_t owner_id;
};

} // namespace idlekv