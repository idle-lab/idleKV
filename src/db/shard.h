#pragma once

#include "common/logger.h"
#include "db/db.h"
#include "db/xmalloc.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mimalloc.h>

namespace idlekv {

using ShardId                     = uint16_t;
constexpr ShardId kInvalidShardId = std::numeric_limits<ShardId>::max();

class Shard {
public:
    Shard(mi_heap_t* heap, ShardId id, size_t DbNum)
        : mr_(heap), id_(id) {
        for (size_t i = 0; i < DbNum; ++i) {
            db_slice_.emplace_back(std::make_shared<DB>(&mr_));
        }

    }

    Shard(const Shard&)                    = delete;
    auto operator=(const Shard&) -> Shard& = delete;

    auto Id() const -> ShardId { return id_; }
    auto DbAt(size_t index) -> DB* {
        CHECK_LT(index, db_slice_.size());
        return db_slice_[index].get();
    }

private:
    std::vector<std::shared_ptr<DB>> db_slice_;
    XAllocator                       mr_;

    ShardId id_;
};

} // namespace idlekv
