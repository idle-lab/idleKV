#pragma once

#include "db/shard.h"
#include "redis/connection.h"

namespace idlekv {

class ExecContext {
public:
    ExecContext(Connection* conn, size_t OwnerId) : conn_(conn), owner_id_(OwnerId) {
        db_index_ = conn_->DbIndex();
    }

    auto GetConnection() -> Connection* { return conn_; }

    auto InitShard(Shard* shard) -> void { shard_ = shard; }

    auto GetDb() -> DB* { return shard_->DbAt(db_index_); }
    auto GetShard() -> Shard* { return shard_; }
    auto OwnerId() -> size_t { return owner_id_; }

private:
    Connection* conn_;

    Shard* shard_;
    size_t db_index_;
    size_t owner_id_;
};

} // namespace idlekv