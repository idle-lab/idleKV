#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>

namespace idlekv::dash::detail {

template <class Key, class Value>
struct Record {
    using KeyType   = Key;
    using ValueType = Value;

    template <class K, class V>
    Record(uint64_t h, uint16_t home, K&& k, V&& v)
        : hash(h), home_bucket(home), key(std::forward<K>(k)), value(std::forward<V>(v)) {}

    uint64_t hash        = 0;
    uint16_t home_bucket = 0;
    Key      key;
    Value    value;
};

template <class RecordType, size_t SlotCount>
class Bucket {
public:
    using RecordPtr = std::shared_ptr<RecordType>;

    static constexpr size_t kSlotCount = SlotCount;

    Bucket()                                = default;
    Bucket(const Bucket&)                   = delete;
    auto operator=(const Bucket&) -> Bucket& = delete;

    auto read_snapshot() const -> uint64_t {
        return version_.load(std::memory_order_acquire);
    }

    auto validate_read(uint64_t snapshot) const -> bool {
        return !(snapshot & 1U) &&
               version_.load(std::memory_order_acquire) == snapshot;
    }

    auto snapshot_slots() const -> std::array<RecordPtr, SlotCount> {
        std::array<RecordPtr, SlotCount> res{};
        for (size_t i = 0; i < SlotCount; ++i) {
            res[i] = std::atomic_load_explicit(&slots_[i], std::memory_order_acquire);
        }
        return res;
    }

    auto load(size_t slot) const -> RecordPtr {
        return std::atomic_load_explicit(&slots_[slot], std::memory_order_acquire);
    }

    auto occupancy() const -> size_t {
        size_t used = 0;
        for (size_t i = 0; i < SlotCount; ++i) {
            used += static_cast<bool>(load(i));
        }
        return used;
    }

    auto first_empty_slot() const -> int {
        for (size_t i = 0; i < SlotCount; ++i) {
            if (!load(i)) {
                return static_cast<int>(i);
            }
        }
        return -1;
    }

    void store(size_t slot, RecordPtr ptr) {
        std::atomic_store_explicit(&slots_[slot], std::move(ptr), std::memory_order_release);
    }

    void reset(size_t slot) {
        std::atomic_store_explicit(&slots_[slot], RecordPtr{}, std::memory_order_release);
    }

    void lock() {
        uint64_t expected = version_.load(std::memory_order_relaxed);
        while (true) {
            if (expected & 1U) {
                std::this_thread::yield();
                expected = version_.load(std::memory_order_relaxed);
                continue;
            }
            if (version_.compare_exchange_weak(expected, expected + 1, std::memory_order_acquire,
                                               std::memory_order_relaxed)) {
                return;
            }
        }
    }

    void unlock() {
        version_.fetch_add(1, std::memory_order_release);
    }

private:
    mutable std::atomic<uint64_t>           version_{0};
    mutable std::array<RecordPtr, SlotCount> slots_{};
};

} // namespace idlekv::dash::detail
