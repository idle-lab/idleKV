#pragma once

#include "common/logger.h"
#include "db/common.h"

#include <absl/container/flat_hash_map.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
namespace idlekv {

// Shared - can be acquired multiple times as long as other intents are absent.
// Exclusive - is acquired only if it's the only lock recorded.
// Transactions at the head of tx-queue are considered to be the ones that acquired the lock
struct Lock {
    enum Mode {
        Shared    = 0,
        Exclusive = 1,
    };

    bool Acquire(Mode mode) {
        C[mode]++;

        if (C[1 ^ int(mode)]) {
            return false;
        }
        return mode == Shared || C[Exclusive] == 1;
    }

    void Release(Mode m, unsigned val = 1) {
        assert(C[m] >= val);

        C[m] -= val;
    }

    bool IsFree() { return (C[0] | C[1]) == 0; }

    uint32_t C[2]{0, 0};
};

class LockTable {
public:
    size_t Size() const { return locks_.size(); }

    bool Acquire(KeyFingerprint fp, Lock::Mode mode) { return locks_[fp].Acquire(mode); }

    void Release(KeyFingerprint fp, Lock::Mode mode) {
        auto it = locks_.find(fp);
        CHECK(it != locks_.end()) << fp;

        it->second.Release(mode);
        if (it->second.IsFree())
            locks_.erase(it);
    }

private:
    // We use fingerprinting before accessing locks - no need to mix more.
    struct Hasher {
        size_t operator()(KeyFingerprint val) const { return val; }
    };
    absl::flat_hash_map<KeyFingerprint, Lock, Hasher> locks_;
};

} // namespace idlekv