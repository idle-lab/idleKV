#pragma once

#include "db/storage/dash/dash.h"
#include "db/storage/result.h"
#include "result.h"

#include <functional>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>

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

template <class K, class V>
class DummyImpl {
public:
    using KeyType = K;
    using ValueType = V;
    using MapType =
        std::unordered_map<KeyType, ValueType, std::hash<KeyType>, std::equal_to<KeyType>, 
                            std::pmr::polymorphic_allocator<std::pair<const KeyType, ValueType>>>;
    
    DummyImpl(std::pmr::memory_resource* mr_) : data_(mr_) {}

    template <class X, class Y>
    auto set_impl(X&& key, Y&& value) -> Result<bool> {
        data_.insert_or_assign(KeyType(std::forward<X>(key)), ValueType(std::forward<Y>(value)));
        return {OpStatus::OK, true};
    }

    template <class X>
    auto get_impl(X&& key) -> Result<ValueType> {
        auto it = data_.find(key);
        if (it == data_.end()) {
            return {OpStatus::OK, ValueType{}};
        }
        return {OpStatus::OK, it->second};
    }

    template <class X>
    auto del_impl(X&& key) -> Result<bool> {
        return {OpStatus::OK, data_.erase(key) != 0};
    }

private:
    MapType data_;

    std::mutex mu_;
};

template <class KeyType, class ValueType, class Hash = std::hash<KeyType>,
          class KeyEqual = std::equal_to<KeyType>>
class DashImpl {
public:
    using TableType   = dash::DashEH<KeyType, ValueType, Hash, KeyEqual>;

    explicit DashImpl(std::pmr::memory_resource* mr) : mr_(mr) {}

    template <class U, class V>
    auto set_impl(U&& key, V&& value) -> Result<bool> {
        KeyType   owned_key(std::forward<U>(key));
        ValueType owned_value(std::forward<V>(value));

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
    auto get_impl(U&& key) -> Result<ValueType> {
        auto record = data_.find_record(KeyType(std::forward<U>(key)));
        if (!record) {
            return {OpStatus::OK, ValueType{}};
        }

        return {OpStatus::OK,
                ConstValueRef(&record->value, std::static_pointer_cast<const void>(record))};
    }

    template <class U>
    auto del_impl(U&& key) -> Result<bool> {
        return {OpStatus::OK, data_.erase(KeyType(std::forward<U>(key)))};
    }

private:
    TableType                  data_;
    std::pmr::memory_resource* mr_ = nullptr;
};

} // namespace idlekv
