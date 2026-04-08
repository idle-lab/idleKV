#pragma once

#include "absl/container/flat_hash_map.h"
#include "db/storage/result.h"
#include "result.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory_resource>
#include <optional>
#include <string_view>
#include <utility>
#include <xxhash.h>

namespace idlekv {

template <class Value>
class KvStore { 
public:
    using KeyType   = std::string;
    using ValueType = Value;

    using MapType =
        absl::flat_hash_map<KeyType, ValueType, std::hash<KeyType>, std::equal_to<KeyType>,
                            std::pmr::polymorphic_allocator<std::pair<const KeyType, Value>>>;
    using Iterator = MapType::iterator;
    KvStore(std::pmr::memory_resource* mr) : data_(mr) {}

    template <class V>
    auto Set(std::string key, V&& value) -> Result<void> {
        data_[std::move(key)] = std::forward<V>(value);
        return {OpStatus::OK};
    }

    auto Get(std::string_view key) -> Result<ValueType> {
        auto it = data_.find(std::string(key));
        if (it == data_.end()) {
            return {OpStatus::NoSuchKey, nullptr};
        }
        return {OpStatus::OK, it->second};
    }

    auto Del(std::string_view key) -> Result<void> {
        auto count = data_.erase(std::string(key));
        if (count == 0) {
            return {OpStatus::NoSuchKey};
        }
        return {OpStatus::OK};
    }

private:
    MapType data_;
};

} // namespace idlekv
