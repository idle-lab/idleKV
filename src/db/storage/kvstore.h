#pragma once

#include "db/storage/result.h"
#include "result.h"
#include <functional>
#include <memory_resource>
#include <mutex>
#include <unordered_map>
#include <utility>


namespace idlekv {

class DataEntity {

};

template<class Impl>
class KvStore {
    using ItType = Impl::ItType;
    using ConstItType = Impl::ConstItType;
public:
    KvStore(std::pmr::memory_resource* mr) : data_(mr), mr_(mr) { }

    template<class U, class V>
    auto set(U&& key, V&& value) -> Result<bool> {
        return data_.set_impl(key, value);
    }

    template<class U>
    auto get(U&& key) -> Result<ConstItType>  {
        return data_.get_impl(key);
    }

    virtual ~KvStore() = default;
private:
    Impl data_;

    std::pmr::memory_resource* mr_;
};

template <class KeyType, class ValueType>
class DummyImpl {
public:
    using MapType = std::unordered_map<KeyType, ValueType, std::hash<KeyType>, std::equal_to<KeyType>, std::pmr::polymorphic_allocator<std::pair<const KeyType, ValueType>>>;
    using ConstItType = typename  MapType::const_iterator;
    using ItType = typename MapType::iterator;

    DummyImpl(std::pmr::memory_resource* mr_) : data_(mr_) {}

    template<class U, class V>
    auto set_impl(U&& key, V&& value) -> Result<bool> {
        std::lock_guard<std::mutex> lk(mu_);
        data_.emplace(std::forward<U>(key), std::forward<V>(value));
        return {OpStatus::OK, true};
    }

    template<class U>
    auto get_impl(U&& key) -> Result<ConstItType> {
        std::lock_guard<std::mutex> lk(mu_);
        return {OpStatus::OK, data_.find(key)};
    }

private:
    MapType data_;

    std::mutex mu_;
};

} // namespace idlekv
