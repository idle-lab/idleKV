#pragma once

#include "db/db.h"

#include <cstdint>
#include <memory>

namespace idlekv {

using ShardId = uint8_t;

class Shard {
public:
private:
    std::vector<std::shared_ptr<DB>> db_slice_;

    ShardId id_;
};

} // namespace idlekv