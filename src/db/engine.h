#pragma once

#include "db/db.h"
#include <memory>
#include <vector>

namespace idlekv {

// IdleEngine is a kv store engine with full capabilities including multiple database, rdb loader
class IdleEngine {
public:
    

private:

    std::vector<std::shared_ptr<DB>> db_set_;
};

} // namespace idlekv
