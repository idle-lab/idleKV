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
        size_t HomeBucket = 0;
        bool   in_stash    = false;
    };

    enum class InsertStatus : uint8_t {
        kInserted,
        kDuplicate,
        kFull,
        kRetry,
    };

    struct InsertResult {
        InsertStatus            status = InsertStatus::kRetry;
        std::optional<Location> location;
    };

    struct EraseResult {
        bool erased = false;
        bool retry  = false;
    };

    explicit Segment(size_t LocalDepth) : local_depth_(LocalDepth) {}

    Segment(const Segment&)                    = delete;
    auto operator=(const Segment&) -> Segment& = delete;

    static constexpr auto Capacity() -> size_t { return kBucketCount * kSlotsPerBucket; }

    auto LocalDepth() const -> size_t { return local_depth_; }

    auto Size() const -> size_t { return size_.load(std::memory_order_acquire); }

    auto LoadFactor() const -> double {
        return static_cast<double>(Size()) / static_cast<double>(Capacity());
    }

    auto Frozen() const -> bool { return frozen_.load(std::memory_order_acquire); }

    auto TryFreeze() -> bool {
        bool expected = false;
        return frozen_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                               std::memory_order_acquire);
    }

    void Unfreeze() { frozen_.store(false, std::memory_order_release); }

    void LockAllBuckets() {
        for (auto& bucket : buckets_) {
            bucket.Lock();
        }
    }

    void UnlockAllBuckets() {
        for (auto it = buckets_.rbegin(); it != buckets_.rend(); ++it) {
            it->Unlock();
        }
    }

    template <class Eq>
    auto Find(const Key& key, uint64_t hash, const Eq& eq) const -> std::optional<Value> {
        auto res = Lookup(key, hash, eq);
        if (!res.record) {
            return std::nullopt;
        }
        return res.record->value;
    }

    template <class Eq>
    auto FindRecord(const Key& key, uint64_t hash, const Eq& eq) const -> RecordPtr {
        return Lookup(key, hash, eq).record;
    }

    template <class Eq>
    auto Locate(const Key& key, uint64_t hash, const Eq& eq) const -> std::optional<Location> {
        return Lookup(key, hash, eq).location;
    }

    template <class Eq, class K, class V>
    auto Insert(K&& key, V&& value, uint64_t hash, const Eq& eq) -> InsertResult {
        RecordType rec(hash, static_cast<uint16_t>(HomeBucket(hash)), std::forward<K>(key),
                       std::forward<V>(value));
        return InsertRecord<true>(rec, eq);
    }

    template <class Eq>
    auto Erase(const Key& key, uint64_t hash, const Eq& eq) -> EraseResult {
        const size_t  home = HomeBucket(hash);
        BucketLockSet locks(this, EraseLockIndices(home));
        (void)locks;
        if (Frozen()) {
            return {.erased = false, .retry = true};
        }

        auto loc = FindDuplicateLocked(home, key, hash, eq);
        if (!loc) {
            return {};
        }

        buckets_[loc->bucket].Reset(loc->slot);
        size_.fetch_sub(1, std::memory_order_acq_rel);
        RebalanceAfterEraseLocked(home);
        return {.erased = true, .retry = false};
    }

    auto SnapshotRecordsLocked() const -> std::vector<RecordType> {
        std::vector<RecordType> records;
        records.reserve(Size());
        for (const auto& bucket : buckets_) {
            for (size_t slot = 0; slot < kSlotsPerBucket; ++slot) {
                if (auto record = bucket.Load(slot); record) {
                    records.push_back(*record);
                }
            }
        }
        return records;
    }

    template <class Eq>
    auto RebuildFrom(const std::vector<RecordType>& records, const Eq& eq) -> bool {
        (void)eq;
        for (const auto& record : records) {
            auto res = InsertRecord<false>(record, eq);
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
                owner_->buckets_[index].Lock();
            }
        }

        BucketLockSet(const BucketLockSet&)                    = delete;
        auto operator=(const BucketLockSet&) -> BucketLockSet& = delete;

        ~BucketLockSet() {
            for (auto it = indices_.rbegin(); it != indices_.rend(); ++it) {
                owner_->buckets_[*it].Unlock();
            }
        }

    private:
        Segment*            owner_;
        std::vector<size_t> indices_;
    };

    static auto NextBucket(size_t bucket) -> size_t {
        return bucket + 1 < kRegularBucketCount ? bucket + 1 : 0;
    }

    static auto PrevBucket(size_t bucket) -> size_t {
        return bucket == 0 ? kRegularBucketCount - 1 : bucket - 1;
    }

    static auto StashBucket(size_t stash_pos) -> size_t { return kRegularBucketCount + stash_pos; }

    static auto HomeBucket(uint64_t hash) -> size_t { return (hash >> 8U) % kRegularBucketCount; }

    auto InsertLockIndices(size_t home) const -> std::vector<size_t> {
        const size_t neighbor = NextBucket(home);
        const size_t next2    = NextBucket(neighbor);
        const size_t previous = PrevBucket(home);

        std::vector<size_t> indices = {previous, home, neighbor, next2};
        indices.reserve(4 + kStashBucketCount);
        for (size_t i = 0; i < kStashBucketCount; ++i) {
            indices.push_back(StashBucket(i));
        }
        return indices;
    }

    auto EraseLockIndices(size_t home) const -> std::vector<size_t> {
        std::vector<size_t> indices = {home, NextBucket(home)};
        indices.reserve(2 + kStashBucketCount);
        for (size_t i = 0; i < kStashBucketCount; ++i) {
            indices.push_back(StashBucket(i));
        }
        return indices;
    }

    template <class Eq>
    auto Lookup(const Key& key, uint64_t hash, const Eq& eq) const -> LookupResult {
        const uint16_t home = static_cast<uint16_t>(HomeBucket(hash));
        const size_t   next = NextBucket(home);

        while (true) {
            if (auto res = LookupBucketOnce(home, key, hash, home, eq); res.retry) {
                continue;
            } else if (res.record) {
                return res;
            }

            if (auto res = LookupBucketOnce(next, key, hash, home, eq); res.retry) {
                continue;
            } else if (res.record) {
                return res;
            }

            bool restart = false;
            for (size_t i = 0; i < kStashBucketCount; ++i) {
                auto res = LookupBucketOnce(StashBucket(i), key, hash, home, eq);
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
    auto LookupBucketOnce(size_t bucket_index, const Key& key, uint64_t hash, uint16_t home,
                          const Eq& eq) const -> LookupResult {
        const auto snapshot = buckets_[bucket_index].ReadSnapshot();
        if (snapshot & 1U) {
            return {.retry = true, .location = std::nullopt, .record = {}};
        }

        const auto slots = buckets_[bucket_index].SnapshotSlots();
        if (!buckets_[bucket_index].ValidateRead(snapshot)) {
            return {.retry = true, .location = std::nullopt, .record = {}};
        }

        for (size_t slot = 0; slot < kSlotsPerBucket; ++slot) {
            const auto& record = slots[slot];
            if (!record || record->hash != hash || record->HomeBucket != home) {
                continue;
            }
            if (eq(record->key, key)) {
                return {
                    .retry = false,
                    .location =
                        Location{
                            .bucket      = bucket_index,
                            .slot        = slot,
                            .HomeBucket = home,
                            .in_stash    = bucket_index >= kRegularBucketCount,
                        },
                    .record = record,
                };
            }
        }

        return {};
    }

    template <class Eq>
    auto FindDuplicateLocked(size_t home, const Key& key, uint64_t hash, const Eq& eq) const
        -> std::optional<Location> {
        const size_t neighbor = NextBucket(home);

        for (size_t bucket_index : {home, neighbor}) {
            for (size_t slot = 0; slot < kSlotsPerBucket; ++slot) {
                auto record = buckets_[bucket_index].Load(slot);
                if (!record || record->hash != hash || record->HomeBucket != home) {
                    continue;
                }
                if (eq(record->key, key)) {
                    return Location{
                        .bucket      = bucket_index,
                        .slot        = slot,
                        .HomeBucket = home,
                        .in_stash    = false,
                    };
                }
            }
        }

        for (size_t i = 0; i < kStashBucketCount; ++i) {
            const size_t bucket_index = StashBucket(i);
            for (size_t slot = 0; slot < kSlotsPerBucket; ++slot) {
                auto record = buckets_[bucket_index].Load(slot);
                if (!record || record->hash != hash || record->HomeBucket != home) {
                    continue;
                }
                if (eq(record->key, key)) {
                    return Location{
                        .bucket      = bucket_index,
                        .slot        = slot,
                        .HomeBucket = home,
                        .in_stash    = true,
                    };
                }
            }
        }

        return std::nullopt;
    }

    auto FirstEmptySlotLocked(size_t bucket_index) const -> int {
        return buckets_[bucket_index].FirstEmptySlot();
    }

    auto PlaceRecordLocked(size_t bucket_index, const RecordPtr& record)
        -> std::optional<Location> {
        const int slot = FirstEmptySlotLocked(bucket_index);
        if (slot < 0) {
            return std::nullopt;
        }

        buckets_[bucket_index].Store(static_cast<size_t>(slot), record);
        return Location{
            .bucket      = bucket_index,
            .slot        = static_cast<size_t>(slot),
            .HomeBucket = record->HomeBucket,
            .in_stash    = bucket_index >= kRegularBucketCount,
        };
    }

    template <class Pred>
    auto MoveFirstLocked(size_t from_bucket, size_t to_bucket, Pred&& pred) -> bool {
        const int target_slot = FirstEmptySlotLocked(to_bucket);
        if (target_slot < 0) {
            return false;
        }

        for (size_t slot = 0; slot < kSlotsPerBucket; ++slot) {
            auto record = buckets_[from_bucket].Load(slot);
            if (!record || !pred(*record)) {
                continue;
            }
            buckets_[to_bucket].Store(static_cast<size_t>(target_slot), record);
            buckets_[from_bucket].Reset(slot);
            return true;
        }
        return false;
    }

    auto PreferredRegularBucket(size_t home, size_t neighbor) const -> std::optional<size_t> {
        const int home_slot     = FirstEmptySlotLocked(home);
        const int neighbor_slot = FirstEmptySlotLocked(neighbor);
        if (home_slot < 0 && neighbor_slot < 0) {
            return std::nullopt;
        }
        if (home_slot < 0) {
            return neighbor;
        }
        if (neighbor_slot < 0) {
            return home;
        }

        const auto home_load     = buckets_[home].Occupancy();
        const auto neighbor_load = buckets_[neighbor].Occupancy();
        return neighbor_load < home_load ? neighbor : home;
    }

    void RebalanceAfterEraseLocked(size_t home) {
        const size_t neighbor = NextBucket(home);

        while (FirstEmptySlotLocked(home) >= 0) {
            if (!MoveFirstLocked(neighbor, home, [home](const RecordType& record) {
                    return record.HomeBucket == home;
                })) {
                break;
            }
        }

        while (true) {
            auto target = PreferredRegularBucket(home, neighbor);
            if (!target) {
                return;
            }

            bool moved = false;
            for (size_t offset = 0; offset < kStashBucketCount; ++offset) {
                const size_t stash_index = StashBucket((home + offset) % kStashBucketCount);
                for (size_t slot = 0; slot < kSlotsPerBucket; ++slot) {
                    auto record = buckets_[stash_index].Load(slot);
                    if (!record || record->HomeBucket != home) {
                        continue;
                    }

                    auto destination = PreferredRegularBucket(home, neighbor);
                    if (!destination) {
                        return;
                    }
                    auto placed = PlaceRecordLocked(*destination, record);
                    if (!placed) {
                        return;
                    }
                    buckets_[stash_index].Reset(slot);
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
    auto InsertRecord(const RecordType& record, const Eq& eq) -> InsertResult {
        const size_t home     = record.HomeBucket;
        const size_t neighbor = NextBucket(home);
        const size_t next2    = NextBucket(neighbor);
        const size_t previous = PrevBucket(home);

        BucketLockSet locks(this, InsertLockIndices(home));
        (void)locks;
        if (Frozen()) {
            return {.status = InsertStatus::kRetry, .location = std::nullopt};
        }

        if constexpr (CheckDuplicate) {
            if (auto duplicate = FindDuplicateLocked(home, record.key, record.hash, eq);
                duplicate) {
                return {.status = InsertStatus::kDuplicate, .location = duplicate};
            }
        }

        auto owned = std::make_shared<RecordType>(record);
        if (auto target = PreferredRegularBucket(home, neighbor); target) {
            auto location = PlaceRecordLocked(*target, owned);
            if (location) {
                size_.fetch_add(1, std::memory_order_acq_rel);
                return {.status = InsertStatus::kInserted, .location = location};
            }
        }

        if (MoveFirstLocked(neighbor, next2, [neighbor](const RecordType& candidate) {
                return candidate.HomeBucket == neighbor;
            })) {
            auto location = PlaceRecordLocked(neighbor, owned);
            if (location) {
                size_.fetch_add(1, std::memory_order_acq_rel);
                return {.status = InsertStatus::kInserted, .location = location};
            }
        }

        if (MoveFirstLocked(home, previous, [previous](const RecordType& candidate) {
                return candidate.HomeBucket == previous;
            })) {
            auto location = PlaceRecordLocked(home, owned);
            if (location) {
                size_.fetch_add(1, std::memory_order_acq_rel);
                return {.status = InsertStatus::kInserted, .location = location};
            }
        }

        for (size_t offset = 0; offset < kStashBucketCount; ++offset) {
            const size_t stash_index = StashBucket((home + offset) % kStashBucketCount);
            auto         location    = PlaceRecordLocked(stash_index, owned);
            if (location) {
                size_.fetch_add(1, std::memory_order_acq_rel);
                return {.status = InsertStatus::kInserted, .location = location};
            }
        }

        return {.status = InsertStatus::kFull, .location = std::nullopt};
    }

    std::array<BucketType, kBucketCount> buckets_{};
    const size_t                         local_depth_;
    std::atomic<size_t>                  size_{0};
    std::atomic<bool>                    frozen_{false};
};

} // namespace idlekv::dash::detail
