#pragma once

#include "common/config.h"
#include "common/logger.h"
#include "db/db.h"
#include "db/task_queue.h"
#include "db/xmalloc.h"
#include "utils/block_queue/block_queue.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mimalloc.h>
#include <queue>
#include <string>
#include <utility>

namespace idlekv {

using ShardId                     = uint16_t;
constexpr ShardId kInvalidShardId = std::numeric_limits<ShardId>::max();

class Shard {
public:
    Shard(mi_heap_t* heap, ShardId id, size_t db_num)
        : mr_(heap), tq_(std::string("shard-") + std::to_string(id)), id_(id) {
        for (size_t i = 0; i < db_num; ++i) {
            db_slice_.emplace_back(std::make_shared<DB>(&mr_));
        }

        tq_.start();
    }

    Shard(const Shard&)                    = delete;
    auto operator=(const Shard&) -> Shard& = delete;

    template <class Fn>
        requires std::invocable<Fn>
    auto dispatch(Fn&& task) -> void {
        tq_.add(std::forward<Fn>(task));
    }

    auto id() const -> ShardId { return id_; }
    auto db_at(size_t index) -> std::shared_ptr<DB> {
        CHECK_LT(index, db_slice_.size());
        return db_slice_[index];
    }

    ~Shard() { tq_.close(); }
private:
    std::vector<std::shared_ptr<DB>> db_slice_;
    XAllocator                       mr_;

    TaskQueue tq_;

    ShardId id_;
};

} // namespace idlekv
