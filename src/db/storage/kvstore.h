#pragma once

#include "absl/container/flat_hash_map.h"
#include "db/storage/art/art.h"
#include "db/storage/art/art_key.h"
#include "db/storage/result.h"
#include "result.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <xxhash.h>

namespace idlekv {

template <class Impl>
class KvStore {
public:
    using ValueType = typename Impl::ValueType;

    KvStore() {}
    explicit KvStore(std::pmr::memory_resource* mr) {}

    template <class U, class V>
    auto Set(U&& key, V&& value) -> Result<bool> {
        return data_.SetImpl(key, value);
    }

    template <class U>
    auto Get(U&& key) -> Result<ValueType> {
        return data_.GetImpl(key);
    }

    template <class U>
    auto Del(U&& key) -> Result<bool> {
        return data_.DelImpl(key);
    }

    auto MemoryUsage() -> size_t;

    virtual ~KvStore() = default;

private:
    Impl data_;
};

template <class Key, class Value>
class DummyImpl {
public:
    using KeyType   = Key;
    using ValueType = Value;

    template <size_t ShardNum>
    class ShardHash {
    public:
        using MapType =
            absl::flat_hash_map<KeyType, ValueType, std::hash<KeyType>, std::equal_to<KeyType>, std::pmr::polymorphic_allocator<std::pair<const KeyType, Value>>>;

        ShardHash() = default;

        template <class U, class V>
        auto Insert(U&& key, V&& value) -> void {
            auto shard_id = Hash(key) % ShardNum;
            {
                std::lock_guard<std::mutex> lg(locks_[shard_id]);
                shards_[shard_id].insert(
                    std::make_pair(std::forward<U>(key), std::forward<V>(value)));
            }
        }

        template <class U>
        auto Find(U&& key) -> std::optional<ValueType> {
            auto shard_id = Hash(key) % ShardNum;
            {
                std::lock_guard<std::mutex> lg(locks_[shard_id]);
                auto                        it = shards_[shard_id].find(key);
                if (it == shards_[shard_id].end()) {
                    return std::nullopt;
                }
                return it->second;
            }
        }

        template <class U>
        auto Erase(U&& key) -> std::optional<size_t> {
            auto shard_id = Hash(key) % ShardNum;
            {
                std::lock_guard<std::mutex>  lg(locks_[shard_id]);
                std::unordered_map<int, int> a;
                a.erase(1);
                return shards_[shard_id].erase(key);
            }
        }

    private:
        auto Hash(const KeyType& key) -> uint64_t { return XXH32(key.data(), key.size(), 54188); }

        std::array<MapType, ShardNum>    shards_;
        std::array<std::mutex, ShardNum> locks_;
    };

    template <class U, class V>
    auto SetImpl(U&& key, V&& value) -> Result<bool> {
        data_.Insert(std::forward<U>(key), std::forward<V>(value));
        return {OpStatus::OK, true};
    }

    template <class U>
    auto GetImpl(U&& key) -> Result<ValueType> {
        auto record = data_.Find(key);
        if (!record.has_value()) {
            return {OpStatus::NoSuchKey, ValueType{}};
        }
        return {OpStatus::OK, record.value()};
    }

    template <class U>
    auto DelImpl(U&& key) -> Result<bool> {
        auto count = data_.Erase(std::forward<U>(key));
        if (count == 0) {
            return {OpStatus::NoSuchKey, true};
        }
        return {OpStatus::OK, true};
    }

private:
    ShardHash<32> data_;
};

// template <class Key, class Value>
// class ArtImpl {
// public:
//     using KeyType = Key;
//     using ValueType = Value;

//     using TableType = Art<ValueType>;

//     explicit ArtImpl()  {}

//     template <class U, class V>
//     auto SetImpl(U&& key, V&& value) -> Result<bool> {
//         ArtKey art_key = BuildArtKey(key);
//         InsertResutl res = tree_.Insert(art_key, std::forward<V>(value), InsertMode::Upsert);
//         switch (res) {
//         case InsertResutl::OK:
//         case InsertResutl::UpsertValue:
//             return {OpStatus::OK, true};
//         case InsertResutl::DuplicateKey:
//             return {OpStatus::DupKey, false};
//         default:
//             return {OpStatus::Unknown, false};
//         }
//     }

//     template <class U>
//     auto GetImpl(U&& key) -> Result<ValueType> {
//         ArtKey art_key = BuildArtKey(key);
//         auto record = tree_.Lookup(art_key);
//         if (!record.has_value()) {
//             return {OpStatus::NoSuchKey, ValueType{}};
//         }
//         return {OpStatus::OK, record.value()};
//     }

//     template <class U>
//     auto DelImpl(U&& key) -> Result<bool> {
//         ArtKey art_key = BuildArtKey(key);
//         size_t erased = tree_.Erase(art_key);
//         if (erased == 0) {
//             return {OpStatus::NoSuchKey, true};
//         }
//         return {OpStatus::OK, true};
//     }

// private:
//     static constexpr uint64_t kSeed = 0x9E3779B97F4A7C15ULL;
//     template <class U>
//     inline auto BuildArtKey(U&& key) -> ArtKey {
//         static_assert(std::is_constructible_v<std::string_view, U&&>,
//                       "ArtImpl only supports string-like keys.");
//         std::string_view sv = key;
//         cur_hash_ = XXH64(sv.data(), sv.size(), kSeed);
//         return ArtKey(reinterpret_cast<const byte*>(&cur_hash_), 8);
//         // return ArtKey::BuildFromString(std::string_view(std::forward<U>(key)));
//     }

//     uint64_t cur_hash_;
//     TableType tree_;
// };

// template <class Key, class Value, class Hash = std::hash<Key>,
//           class KeyEqual = std::equal_to<Key>>
// class DashImpl {
// public:
//     using KeyType = Key;
//     using ValueType = Value;
//     using TableType   = dash::DashEH<Key, Value, Hash, KeyEqual>;

//     explicit DashImpl(std::pmr::memory_resource* mr) : mr_(mr) {}

//     template <class U, class V>
//     auto SetImpl(U&& key, V&& value) -> Result<bool> {
//         Key   owned_key(std::forward<U>(key));
//         Value owned_value(std::forward<V>(value));

//         while (true) {
//             if (data_.Insert(owned_key, owned_value)) {
//                 return {OpStatus::OK, true};
//             }

//             auto erased = data_.Erase(owned_key);
//             if (!erased) {
//                 continue;
//             }
//         }
//     }

//     template <class U>
//     auto GetImpl(U&& key) -> Result<Value> {
//         auto record = data_.FindRecord(Key(std::forward<U>(key)));
//         if (!record) {
//             return {OpStatus::OK, Value{}};
//         }

//         return {OpStatus::OK, record->value};
//     }

//     template <class U>
//     auto DelImpl(U&& key) -> Result<bool> {
//         return {OpStatus::OK, data_.Erase(Key(std::forward<U>(key)))};
//     }

// private:
//     TableType                  data_;
//     std::pmr::memory_resource* mr_ = nullptr;
// };

} // namespace idlekv
