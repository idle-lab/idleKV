#include "db/db.h"

#include "db/lock.h"
#include "db/storage/result.h"
#include "db/storage/value.h"

#include <absl/container/inlined_vector.h>
#include <string_view>
#include <utility>

namespace idlekv {

DB::DB(std::pmr::memory_resource* mr) : prime_(mr) {}

auto DB::Set(std::string_view key, PrimeValue value) -> Result<void> {
    return prime_.Set(key, std::move(value));
}

auto DB::Get(std::string_view key, Value::TypeEnum type) -> Result<PrimeValue> {
    auto res = prime_.Get(key);
    if (!res.Ok()) {
        return res;
    }

    if (res.payload->Type() != type) {
        return OpStatus::WrongType;
    }
    return res;
}

auto DB::Del(std::string_view key) -> Result<void> { return prime_.Del(key); }

auto DB::AcquireTxnLocks(const std::unordered_set<KeyFingerprint>& read_fps,
                         const std::unordered_set<KeyFingerprint>& write_fps) -> bool {
    bool res = true;
    for (auto fp : write_fps) {
        res &= lock_table_.Acquire(fp, Lock::Exclusive);
    }

    for (auto fp : read_fps) {
        if (write_fps.contains(fp)) {
            continue;
        }
        res &= lock_table_.Acquire(fp, Lock::Shared);
    }

    return res;
}

auto DB::ReleaseTxnLocks(const std::unordered_set<KeyFingerprint>& read_fps,
                         const std::unordered_set<KeyFingerprint>& write_fps) -> void {
    for (auto fp : write_fps) {
        lock_table_.Release(fp, Lock::Exclusive);
    }

    for (auto fp : read_fps) {
        if (write_fps.contains(fp)) {
            continue;
        }
        lock_table_.Release(fp, Lock::Shared);
    }
}

} // namespace idlekv
