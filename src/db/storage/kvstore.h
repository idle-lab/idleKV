#pragma once

#include "absl/container/flat_hash_map.h"
#include "db/storage/result.h"
#include "result.h"

#include <algorithm>
#include <cstddef>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>

namespace idlekv {

template <class Value>
class KvStore {
public:
    using KeyType   = std::string;
    using ValueType = Value;

    using Hash      = absl::container_internal::FlatHashMapPolicy<std::string, Value>::DefaultHash;
    using Eq        = absl::container_internal::FlatHashMapPolicy<std::string, Value>::DefaultEq;
    using Allocator = std::pmr::polymorphic_allocator<std::pair<const KeyType, ValueType>>;
    using MapType   = absl::flat_hash_map<KeyType, ValueType, Hash, Eq, Allocator>;
    using Iterator  = MapType::iterator;

    explicit KvStore(std::pmr::memory_resource* mr) : data_(mr), rehash_data_(mr) {
        rehash_it_ = data_.end();
    }

    template <class V>
    auto Set(std::string_view key, V&& value) -> Result<void> {
        RehashStep();

        if (auto* existing = FindValue(key); existing != nullptr) {
            *existing = std::forward<V>(value);
            return {OpStatus::OK};
        }

        if (!rehashing_ && ShouldGrowForInsert()) {
            StartRehash(NextRehashCapacity());
        }

        ActiveMapForInsert()[key] = std::forward<V>(value);
        return {OpStatus::OK};
    }

    auto Get(std::string_view key) -> Result<ValueType> {
        RehashStep();

        if (auto* value = FindValue(key); value != nullptr) {
            return {OpStatus::OK, *value};
        }

        return {OpStatus::NoSuchKey, ValueType{}};
    }

    auto Del(std::string_view key) -> Result<void> {
        RehashStep();

        if (EraseFromMap(rehash_data_, key)) {
            return {OpStatus::OK};
        }

        if (!EraseFromData(key)) {
            return {OpStatus::NoSuchKey};
        }

        return {OpStatus::OK};
    }

private:
    static constexpr size_t kRehashStepsPerOp      = 1;
    static constexpr size_t kLoadFactorNumerator   = 7;
    static constexpr size_t kLoadFactorDenominator = 8;
    static constexpr size_t kMinReservedSlotCount  = 16;

    auto FindValue(std::string_view key) -> ValueType* {
        if (rehashing_) {
            auto rehash_it = rehash_data_.find(key);
            if (rehash_it != rehash_data_.end()) {
                return &rehash_it->second;
            }
        }

        auto it = data_.find(key);
        if (it == data_.end()) {
            return nullptr;
        }
        return &it->second;
    }

    auto ActiveMapForInsert() -> MapType& { return rehashing_ ? rehash_data_ : data_; }

    auto ShouldGrowForInsert() const -> bool {
        const size_t capacity = data_.capacity();
        if (capacity == 0) {
            return false;
        }

        const size_t used_after_insert = data_.size() + 1;
        return used_after_insert * kLoadFactorDenominator >= capacity * kLoadFactorNumerator;
    }

    auto NextRehashCapacity() const -> size_t {
        const size_t base = std::max(data_.size() + 1, data_.capacity());
        return std::max(kMinReservedSlotCount, base * 2);
    }

    auto StartRehash(size_t target_capacity) -> void {
        if (rehashing_ || data_.empty()) {
            return;
        }

        rehash_data_.clear();
        rehash_data_.reserve(target_capacity);
        rehash_it_ = data_.begin();
        rehashing_ = true;
    }

    auto RehashStep(size_t steps = kRehashStepsPerOp) -> void {
        if (!rehashing_) {
            return;
        }

        // Keep each request bounded by migrating only a handful of entries per operation.
        for (size_t moved = 0; moved < steps && rehash_it_ != data_.end(); ++moved) {
            auto current = rehash_it_++;
            rehash_data_.emplace(current->first, std::move(current->second));
            data_.erase(current);
        }

        FinishRehashIfDone();
    }

    auto FinishRehashIfDone() -> void {
        if (!rehashing_ || !data_.empty()) {
            return;
        }

        data_.swap(rehash_data_);
        rehash_data_.clear();
        rehash_it_ = data_.end();
        rehashing_ = false;
    }

    auto EraseFromMap(MapType& map, std::string_view key) -> bool {
        auto it = map.find(key);
        if (it == map.end()) {
            return false;
        }

        map.erase(it);
        return true;
    }

    auto EraseFromData(std::string_view key) -> bool {
        auto it = data_.find(key);
        if (it == data_.end()) {
            return false;
        }

        if (rehashing_ && it == rehash_it_) {
            ++rehash_it_;
        }

        data_.erase(it);
        FinishRehashIfDone();
        return true;
    }

    MapType  data_;
    MapType  rehash_data_;
    Iterator rehash_it_;
    bool     rehashing_{false};
};

} // namespace idlekv
