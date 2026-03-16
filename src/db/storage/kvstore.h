#pragma once

#include "db/storage/dash/dash.h"
#include "db/storage/result.h"
#include "result.h"

#include <array>
#include <cstddef>
#include <functional>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <xxhash.h>

namespace idlekv {

class DataEntity {
public:
    enum class Type : uint8_t {
        kString,
    };

    DataEntity() = default;

    static auto from_string(std::string value) -> DataEntity {
        return DataEntity(Type::kString, std::move(value));
    }

    auto type() const -> Type { return type_; }

    auto is_string() const -> bool { return type_ == Type::kString; }

    auto as_string() const -> const std::string& { return string_value_; }

    auto operator==(const DataEntity&) const -> bool = default;

private:
    DataEntity(Type type, std::string value) : type_(type), string_value_(std::move(value)) {}

    Type        type_ = Type::kString;
    std::string string_value_;
};

template <class Impl>
class KvStore {
public:
    using ValueType  = typename Impl::ValueType;

    KvStore(std::pmr::memory_resource* mr) : data_(mr), mr_(mr) {}

    template <class U, class V>
    auto set(U&& key, V&& value) -> Result<bool> {
        return data_.set_impl(key, value);
    }

    template <class U>
    auto get(U&& key) -> Result<ValueType> {
        return data_.get_impl(key);
    }

    template <class U>
    auto del(U&& key) -> Result<bool> {
        return data_.del_impl(key);
    }

    virtual ~KvStore() = default;

private:
    Impl data_;

    std::pmr::memory_resource* mr_;
};

template <class Key, class Value>
class DummyImpl {
public:
    using KeyType = Key;
    using ValueType = Value;

    template<size_t ShardNum>
    class ShardHash {
    public:
    using MapType =
        std::unordered_map<KeyType, ValueType, std::hash<KeyType>, std::equal_to<KeyType>, 
                            std::pmr::polymorphic_allocator<std::pair<const KeyType, ValueType>>>;
    explicit ShardHash([[maybe_unused]] std::pmr::memory_resource* mr_) {}
    
    template <class U, class V>
    auto insert(U&& key, V&& value) -> void {
        auto shard_id = hash(key) % ShardNum;
        {
            std::lock_guard<std::mutex> lg(locks_[shard_id]);
            shards_[shard_id].insert(std::make_pair(std::forward<U>(key), std::forward<V>(value)));
        }
    }

    template <class U>
    auto find(U&& key) -> std::optional<ValueType> {
        auto shard_id = hash(key) % ShardNum;
        {
            std::lock_guard<std::mutex> lg(locks_[shard_id]);
            auto it = shards_[shard_id].find(key);
            if (it == shards_[shard_id].end()) {
                return std::nullopt;
            }
            return it->second;
        }
    }

    template <class U>
    auto earse(U&& key) -> std::optional<size_t> {
        auto shard_id = hash(key) % ShardNum;
        {
            std::lock_guard<std::mutex> lg(locks_[shard_id]);
            std::unordered_map<int, int> a;
            a.erase(1);
            return shards_[shard_id].erase(key);
        }
    }

    private:
        auto hash(const KeyType& key) -> uint64_t {
            return XXH32(key.data(), key.size(), 54188);
        }

        std::array<MapType, ShardNum> shards_;
        std::array<std::mutex, ShardNum> locks_;
    };
    DummyImpl(std::pmr::memory_resource* mr_) : data_(mr_) {}

    template <class U, class V>
    auto set_impl(U&& key, V&& value) -> Result<bool> {
        data_.insert(std::forward<U>(key), std::forward<V>(value));
        return {OpStatus::OK, true};
    }

    template <class U>
    auto get_impl(U&& key) -> Result<ValueType> {
        auto record = data_.find(key);
        if (!record.has_value()) {
            return {OpStatus::NoSuchKey, ValueType{}};
        }
        return {OpStatus::OK, record.value()};
    }

    template <class U>
    auto del_impl(U&& key) -> Result<bool> {
        auto count = data_.earse(std::forward<U>(key));
        if (count == 0) {
            return {OpStatus::NoSuchKey, true};
        }
        return {OpStatus::OK, true};
    }

private:
    ShardHash<32> data_;
};

template <class Key, class Value, class Hash = std::hash<Key>,
          class KeyEqual = std::equal_to<Key>>
class DashImpl {
public:
    using KeyType = Key;
    using ValueType = Value;
    using TableType   = dash::DashEH<Key, Value, Hash, KeyEqual>;

    explicit DashImpl(std::pmr::memory_resource* mr) : mr_(mr) {}

    template <class U, class V>
    auto set_impl(U&& key, V&& value) -> Result<bool> {
        Key   owned_key(std::forward<U>(key));
        Value owned_value(std::forward<V>(value));

        while (true) {
            if (data_.insert(owned_key, owned_value)) {
                return {OpStatus::OK, true};
            }

            auto erased = data_.erase(owned_key);
            if (!erased) {
                continue;
            }
        }
    }

    template <class U>
    auto get_impl(U&& key) -> Result<Value> {
        auto record = data_.find_record(Key(std::forward<U>(key)));
        if (!record) {
            return {OpStatus::OK, Value{}};
        }

        return {OpStatus::OK, record->value};
    }

    template <class U>
    auto del_impl(U&& key) -> Result<bool> {
        return {OpStatus::OK, data_.erase(Key(std::forward<U>(key)))};
    }

private:
    TableType                  data_;
    std::pmr::memory_resource* mr_ = nullptr;
};

} // namespace idlekv
