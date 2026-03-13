#pragma once

#include "db/storage/dash/bucket.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace idlekv::dash::detail {

template <class Key, class Value, size_t RegularBuckets, size_t StashBuckets, size_t SlotsPerBucket>
class Segment {
    static_assert(RegularBuckets >= 2, "Dash segment needs at least two regular buckets");
    static_assert(StashBuckets >= 1, "Dash segment needs stash buckets");
    static_assert(SlotsPerBucket >= 1, "Dash bucket must have at least one slot");

public:
    using RecordType = Record<Key, Value>;
    using RecordPtr  = std::shared_ptr<RecordType>;
    using BucketType = Bucket<RecordType, SlotsPerBucket>;

    static constexpr size_t kRegularBucketCount = RegularBuckets;
    static constexpr size_t kStashBucketCount   = StashBuckets;
    static constexpr size_t kBucketCount        = RegularBuckets + StashBuckets;
    static constexpr size_t kSlotsPerBucket     = SlotsPerBucket;

    struct Location {
        size_t bucket      = 0;
        size_t slot        = 0;
        size_t home_bucket = 0;
        bool   in_stash    = false;
    };

    enum class InsertStatus : uint8_t {
        kInserted,
        kDuplicate,
        kFull,
        kRetry,
    };

    struct InsertResult {
        InsertStatus             status = InsertStatus::kRetry;
        std::optional<Location>  location;
    };

    struct EraseResult {
        bool erased = false;
        bool retry  = false;
    };

    explicit Segment(size_t local_depth) : local_depth_(local_depth) {}

    Segment(const Segment&)                        = delete;
    auto operator=(const Segment&) -> Segment&    = delete;

    static constexpr auto capacity() -> size_t {
        return kBucketCount * kSlotsPerBucket;
    }

    auto local_depth() const -> size_t { return local_depth_; }

    auto size() const -> size_t { return size_.load(std::memory_order_acquire); }

    auto load_factor() const -> double {
        return static_cast<double>(size()) / static_cast<double>(capacity());
    }

    auto frozen() const -> bool { return frozen_.load(std::memory_order_acquire); }

    auto try_freeze() -> bool {
        bool expected = false;
        return frozen_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                               std::memory_order_acquire);
    }

    void unfreeze() { frozen_.store(false, std::memory_order_release); }

    void lock_all_buckets() {
        for (auto& bucket : buckets_) {
            bucket.lock();
        }
    }

    void unlock_all_buckets() {
        for (auto it = buckets_.rbegin(); it != buckets_.rend(); ++it) {
            it->unlock();
        }
    }

    template <class Eq>
    auto find(const Key& key, uint64_t hash, const Eq& eq) const -> std::optional<Value> {
        auto res = lookup(key, hash, eq);
        if (!res.record) {
            return std::nullopt;
        }
        return res.record->value;
    }

    template <class Eq>
    auto locate(const Key& key, uint64_t hash, const Eq& eq) const -> std::optional<Location> {
        return lookup(key, hash, eq).location;
    }

    template <class Eq, class K, class V>
    auto insert(K&& key, V&& value, uint64_t hash, const Eq& eq) -> InsertResult {
        RecordType rec(hash, static_cast<uint16_t>(home_bucket(hash)), std::forward<K>(key),
                       std::forward<V>(value));
        return insert_record<true>(rec, eq);
    }

    template <class Eq>
    auto erase(const Key& key, uint64_t hash, const Eq& eq) -> EraseResult {
        const size_t home = home_bucket(hash);
        BucketLockSet locks(this, erase_lock_indices(home));
        (void)locks;
        if (frozen()) {
            return {.erased = false, .retry = true};
        }

        auto loc = find_duplicate_locked(home, key, hash, eq);
        if (!loc) {
            return {};
        }

        buckets_[loc->bucket].reset(loc->slot);
        size_.fetch_sub(1, std::memory_order_acq_rel);
        rebalance_after_erase_locked(home);
        return {.erased = true, .retry = false};
    }

    auto snapshot_records_locked() const -> std::vector<RecordType> {
        std::vector<RecordType> records;
        records.reserve(size());
        for (const auto& bucket : buckets_) {
            for (size_t slot = 0; slot < kSlotsPerBucket; ++slot) {
                if (auto record = bucket.load(slot); record) {
                    records.push_back(*record);
                }
            }
        }
        return records;
    }

    template <class Eq>
    auto rebuild_from(const std::vector<RecordType>& records, const Eq& eq) -> bool {
        (void)eq;
        for (const auto& record : records) {
            auto res = insert_record<false>(record, eq);
            if (res.status != InsertStatus::kInserted) {
                return false;
            }
        }
        return true;
    }

private:
    struct LookupResult {
        bool                    retry = false;
        std::optional<Location> location;
        RecordPtr               record;
    };

    class BucketLockSet {
    public:
        BucketLockSet(Segment* owner, std::vector<size_t> indices)
            : owner_(owner), indices_(std::move(indices)) {
            std::sort(indices_.begin(), indices_.end());
            indices_.erase(std::unique(indices_.begin(), indices_.end()), indices_.end());
            for (size_t index : indices_) {
                owner_->buckets_[index].lock();
            }
        }

        BucketLockSet(const BucketLockSet&)                    = delete;
        auto operator=(const BucketLockSet&) -> BucketLockSet& = delete;

        ~BucketLockSet() {
            for (auto it = indices_.rbegin(); it != indices_.rend(); ++it) {
                owner_->buckets_[*it].unlock();
            }
        }

    private:
        Segment*             owner_;
        std::vector<size_t>  indices_;
    };

    static auto next_bucket(size_t bucket) -> size_t {
        return bucket + 1 < kRegularBucketCount ? bucket + 1 : 0;
    }

    static auto prev_bucket(size_t bucket) -> size_t {
        return bucket == 0 ? kRegularBucketCount - 1 : bucket - 1;
    }

    static auto stash_bucket(size_t stash_pos) -> size_t {
        return kRegularBucketCount + stash_pos;
    }

    static auto home_bucket(uint64_t hash) -> size_t {
        return (hash >> 8U) % kRegularBucketCount;
    }

    auto insert_lock_indices(size_t home) const -> std::vector<size_t> {
        const size_t neighbor = next_bucket(home);
        const size_t next2    = next_bucket(neighbor);
        const size_t previous = prev_bucket(home);

        std::vector<size_t> indices = {previous, home, neighbor, next2};
        indices.reserve(4 + kStashBucketCount);
        for (size_t i = 0; i < kStashBucketCount; ++i) {
            indices.push_back(stash_bucket(i));
        }
        return indices;
    }

    auto erase_lock_indices(size_t home) const -> std::vector<size_t> {
        std::vector<size_t> indices = {home, next_bucket(home)};
        indices.reserve(2 + kStashBucketCount);
        for (size_t i = 0; i < kStashBucketCount; ++i) {
            indices.push_back(stash_bucket(i));
        }
        return indices;
    }

    template <class Eq>
    auto lookup(const Key& key, uint64_t hash, const Eq& eq) const -> LookupResult {
        const uint16_t home = static_cast<uint16_t>(home_bucket(hash));
        const size_t   next = next_bucket(home);

        while (true) {
            if (auto res = lookup_bucket_once(home, key, hash, home, eq); res.retry) {
                continue;
            } else if (res.record) {
                return res;
            }

            if (auto res = lookup_bucket_once(next, key, hash, home, eq); res.retry) {
                continue;
            } else if (res.record) {
                return res;
            }

            bool restart = false;
            for (size_t i = 0; i < kStashBucketCount; ++i) {
                auto res = lookup_bucket_once(stash_bucket(i), key, hash, home, eq);
                if (res.retry) {
                    restart = true;
                    break;
                }
                if (res.record) {
                    return res;
                }
            }

            if (!restart) {
                return {};
            }
        }
    }

    template <class Eq>
    auto lookup_bucket_once(size_t bucket_index, const Key& key, uint64_t hash, uint16_t home,
                            const Eq& eq) const -> LookupResult {
        const auto snapshot = buckets_[bucket_index].read_snapshot();
        if (snapshot & 1U) {
            return {.retry = true, .location = std::nullopt, .record = {}};
        }

        const auto slots = buckets_[bucket_index].snapshot_slots();
        if (!buckets_[bucket_index].validate_read(snapshot)) {
            return {.retry = true, .location = std::nullopt, .record = {}};
        }

        for (size_t slot = 0; slot < kSlotsPerBucket; ++slot) {
            const auto& record = slots[slot];
            if (!record || record->hash != hash || record->home_bucket != home) {
                continue;
            }
            if (eq(record->key, key)) {
                return {
                    .retry = false,
                    .location =
                        Location{
                            .bucket      = bucket_index,
                            .slot        = slot,
                            .home_bucket = home,
                            .in_stash    = bucket_index >= kRegularBucketCount,
                        },
                    .record = record,
                };
            }
        }

        return {};
    }

    template <class Eq>
    auto find_duplicate_locked(size_t home, const Key& key, uint64_t hash, const Eq& eq) const
        -> std::optional<Location> {
        const size_t neighbor = next_bucket(home);

        for (size_t bucket_index : {home, neighbor}) {
            for (size_t slot = 0; slot < kSlotsPerBucket; ++slot) {
                auto record = buckets_[bucket_index].load(slot);
                if (!record || record->hash != hash || record->home_bucket != home) {
                    continue;
                }
                if (eq(record->key, key)) {
                    return Location{
                        .bucket      = bucket_index,
                        .slot        = slot,
                        .home_bucket = home,
                        .in_stash    = false,
                    };
                }
            }
        }

        for (size_t i = 0; i < kStashBucketCount; ++i) {
            const size_t bucket_index = stash_bucket(i);
            for (size_t slot = 0; slot < kSlotsPerBucket; ++slot) {
                auto record = buckets_[bucket_index].load(slot);
                if (!record || record->hash != hash || record->home_bucket != home) {
                    continue;
                }
                if (eq(record->key, key)) {
                    return Location{
                        .bucket      = bucket_index,
                        .slot        = slot,
                        .home_bucket = home,
                        .in_stash    = true,
                    };
                }
            }
        }

        return std::nullopt;
    }

    auto first_empty_slot_locked(size_t bucket_index) const -> int {
        return buckets_[bucket_index].first_empty_slot();
    }

    auto place_record_locked(size_t bucket_index, const RecordPtr& record) -> std::optional<Location> {
        const int slot = first_empty_slot_locked(bucket_index);
        if (slot < 0) {
            return std::nullopt;
        }

        buckets_[bucket_index].store(static_cast<size_t>(slot), record);
        return Location{
            .bucket      = bucket_index,
            .slot        = static_cast<size_t>(slot),
            .home_bucket = record->home_bucket,
            .in_stash    = bucket_index >= kRegularBucketCount,
        };
    }

    template <class Pred>
    auto move_first_locked(size_t from_bucket, size_t to_bucket, Pred&& pred) -> bool {
        const int target_slot = first_empty_slot_locked(to_bucket);
        if (target_slot < 0) {
            return false;
        }

        for (size_t slot = 0; slot < kSlotsPerBucket; ++slot) {
            auto record = buckets_[from_bucket].load(slot);
            if (!record || !pred(*record)) {
                continue;
            }
            buckets_[to_bucket].store(static_cast<size_t>(target_slot), record);
            buckets_[from_bucket].reset(slot);
            return true;
        }
        return false;
    }

    auto preferred_regular_bucket(size_t home, size_t neighbor) const -> std::optional<size_t> {
        const int home_slot     = first_empty_slot_locked(home);
        const int neighbor_slot = first_empty_slot_locked(neighbor);
        if (home_slot < 0 && neighbor_slot < 0) {
            return std::nullopt;
        }
        if (home_slot < 0) {
            return neighbor;
        }
        if (neighbor_slot < 0) {
            return home;
        }

        const auto home_load     = buckets_[home].occupancy();
        const auto neighbor_load = buckets_[neighbor].occupancy();
        return neighbor_load < home_load ? neighbor : home;
    }

    void rebalance_after_erase_locked(size_t home) {
        const size_t neighbor = next_bucket(home);

        while (first_empty_slot_locked(home) >= 0) {
            if (!move_first_locked(neighbor, home,
                                   [home](const RecordType& record) {
                                       return record.home_bucket == home;
                                   })) {
                break;
            }
        }

        while (true) {
            auto target = preferred_regular_bucket(home, neighbor);
            if (!target) {
                return;
            }

            bool moved = false;
            for (size_t offset = 0; offset < kStashBucketCount; ++offset) {
                const size_t stash_index = stash_bucket((home + offset) % kStashBucketCount);
                for (size_t slot = 0; slot < kSlotsPerBucket; ++slot) {
                    auto record = buckets_[stash_index].load(slot);
                    if (!record || record->home_bucket != home) {
                        continue;
                    }

                    auto destination = preferred_regular_bucket(home, neighbor);
                    if (!destination) {
                        return;
                    }
                    auto placed = place_record_locked(*destination, record);
                    if (!placed) {
                        return;
                    }
                    buckets_[stash_index].reset(slot);
                    moved = true;
                    break;
                }
                if (moved) {
                    break;
                }
            }

            if (!moved) {
                return;
            }
        }
    }

    template <bool CheckDuplicate, class Eq>
    auto insert_record(const RecordType& record, const Eq& eq) -> InsertResult {
        const size_t home     = record.home_bucket;
        const size_t neighbor = next_bucket(home);
        const size_t next2    = next_bucket(neighbor);
        const size_t previous = prev_bucket(home);

        BucketLockSet locks(this, insert_lock_indices(home));
        (void)locks;
        if (frozen()) {
            return {.status = InsertStatus::kRetry, .location = std::nullopt};
        }

        if constexpr (CheckDuplicate) {
            if (auto duplicate = find_duplicate_locked(home, record.key, record.hash, eq); duplicate) {
                return {.status = InsertStatus::kDuplicate, .location = duplicate};
            }
        }

        auto owned = std::make_shared<RecordType>(record);
        if (auto target = preferred_regular_bucket(home, neighbor); target) {
            auto location = place_record_locked(*target, owned);
            if (location) {
                size_.fetch_add(1, std::memory_order_acq_rel);
                return {.status = InsertStatus::kInserted, .location = location};
            }
        }

        if (move_first_locked(neighbor, next2,
                              [neighbor](const RecordType& candidate) {
                                  return candidate.home_bucket == neighbor;
                              })) {
            auto location = place_record_locked(neighbor, owned);
            if (location) {
                size_.fetch_add(1, std::memory_order_acq_rel);
                return {.status = InsertStatus::kInserted, .location = location};
            }
        }

        if (move_first_locked(home, previous,
                              [previous](const RecordType& candidate) {
                                  return candidate.home_bucket == previous;
                              })) {
            auto location = place_record_locked(home, owned);
            if (location) {
                size_.fetch_add(1, std::memory_order_acq_rel);
                return {.status = InsertStatus::kInserted, .location = location};
            }
        }

        for (size_t offset = 0; offset < kStashBucketCount; ++offset) {
            const size_t stash_index = stash_bucket((home + offset) % kStashBucketCount);
            auto         location    = place_record_locked(stash_index, owned);
            if (location) {
                size_.fetch_add(1, std::memory_order_acq_rel);
                return {.status = InsertStatus::kInserted, .location = location};
            }
        }

        return {.status = InsertStatus::kFull, .location = std::nullopt};
    }

    std::array<BucketType, kBucketCount> buckets_{};
    const size_t                          local_depth_;
    std::atomic<size_t>                   size_{0};
    std::atomic<bool>                     frozen_{false};
};

} // namespace idlekv::dash::detail
