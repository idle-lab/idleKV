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
        : hash(h), HomeBucket(home), key(std::forward<K>(k)), value(std::forward<V>(v)) {}

    uint64_t hash       = 0;
    uint16_t HomeBucket = 0;
    Key      key;
    Value    value;
};

template <class RecordType, size_t SlotCount>
class Bucket {
public:
    using RecordPtr = std::shared_ptr<RecordType>;

    static constexpr size_t kSlotCount = SlotCount;

    Bucket()                                 = default;
    Bucket(const Bucket&)                    = delete;
    auto operator=(const Bucket&) -> Bucket& = delete;

private:
    mutable std::atomic<uint64_t>            version_{0};
    mutable std::array<RecordPtr, SlotCount> slots_{};
};

} // namespace idlekv::dash::detail
