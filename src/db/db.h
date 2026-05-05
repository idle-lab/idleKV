#pragma once

#include "db/common.h"
#include "db/lock.h"
#include "db/storage/kvstore.h"
#include "db/storage/result.h"
#include "db/storage/value.h"
#include "utils/time/time.h"

#include <absl/container/flat_hash_map.h>
#include <memory_resource>
#include <mimalloc.h>
#include <unordered_set>

namespace idlekv {

using PrimeTable   = KvStore<PrimeValue>;
using ExpiredTable = KvStore<TimePoint>;

// DB stores data and execute user's commands
// TODO(c113): ExpiredTable
class DB {
public:
    explicit DB(std::pmr::memory_resource* mr);

    auto Set(std::string_view key, PrimeValue value) -> Result<void>;

    auto Get(std::string_view key, Value::TypeEnum type) -> Result<PrimeValue>;

    auto Del(std::string_view key) -> Result<void>;

    auto AcquireTxnLocks(const std::unordered_set<KeyFingerprint>& read_fps,
                         const std::unordered_set<KeyFingerprint>& write_fps) -> bool;

    auto ReleaseTxnLocks(const std::unordered_set<KeyFingerprint>& read_fps,
                         const std::unordered_set<KeyFingerprint>& write_fps) -> void;

    // TODO(cyb)
    auto MemoryUsage() -> size_t;

private:
    PrimeTable prime_;

    LockTable lock_table_;
};

} // namespace idlekv
