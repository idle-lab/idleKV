#pragma once

#include "db/storage/dash/segment.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace idlekv::dash::detail {

template <class Fn>
class ScopeExit {
public:
    explicit ScopeExit(Fn&& fn) : fn_(std::forward<Fn>(fn)) {}

    ScopeExit(const ScopeExit&)                    = delete;
    auto operator=(const ScopeExit&) -> ScopeExit& = delete;

    ScopeExit(ScopeExit&& other) noexcept : fn_(std::move(other.fn_)), active_(other.active_) {
        other.active_ = false;
    }

    ~ScopeExit() {
        if (active_) {
            fn_();
        }
    }

    void Release() { active_ = false; }

private:
    Fn   fn_;
    bool active_ = true;
};

template <class Fn>
auto MakeScopeExit(Fn&& fn) -> ScopeExit<std::decay_t<Fn>> {
    return ScopeExit<std::decay_t<Fn>>(std::forward<Fn>(fn));
}

class EpochManager {
public:
    EpochManager() = default;

    EpochManager(const EpochManager&)                    = delete;
    auto operator=(const EpochManager&) -> EpochManager& = delete;

    class Guard {
    public:
        Guard() = default;

        Guard(Guard&& other) noexcept : slot_(other.slot_) { other.slot_ = nullptr; }

        auto operator=(Guard&& other) noexcept -> Guard& {
            if (this == &other) {
                return *this;
            }
            Reset();
            slot_       = other.slot_;
            other.slot_ = nullptr;
            return *this;
        }

        Guard(const Guard&)                    = delete;
        auto operator=(const Guard&) -> Guard& = delete;

        ~Guard() { Reset(); }

        void Reset() {
            if (!slot_) {
                return;
            }
            slot_->active.store(false, std::memory_order_release);
            slot_->epoch.store(kInactiveEpoch, std::memory_order_release);
            slot_ = nullptr;
        }

    private:
        friend class EpochManager;

        static constexpr uint64_t kInactiveEpoch = std::numeric_limits<uint64_t>::max();

        struct ThreadEpoch {
            std::atomic<uint64_t> epoch{kInactiveEpoch};
            std::atomic<bool>     active{false};
        };

        explicit Guard(ThreadEpoch* slot, uint64_t epoch) : slot_(slot) {
            slot_->epoch.store(epoch, std::memory_order_release);
            slot_->active.store(true, std::memory_order_release);
        }

        ThreadEpoch* slot_ = nullptr;
    };

    auto Pin() const -> Guard {
        return Guard(GetOrRegisterSlot(), global_epoch_.load(std::memory_order_acquire));
    }

    template <class T>
    void Retire(T* ptr) {
        RetireImpl(ptr, [](void* raw) { delete static_cast<T*>(raw); });
    }

    void DrainAll() {
        std::vector<RetiredNode> pending;
        {
            std::lock_guard<std::mutex> lk(retired_mu_);
            pending.swap(retired_);
        }
        for (const auto& node : pending) {
            node.deleter(node.ptr);
        }
    }

private:
    struct RetiredNode {
        uint64_t retire_epoch  = 0;
        void*    ptr           = nullptr;
        void (*deleter)(void*) = nullptr;
    };

    using ThreadEpoch = Guard::ThreadEpoch;

    auto GetOrRegisterSlot() const -> ThreadEpoch* {
        thread_local std::unordered_map<const EpochManager*, std::weak_ptr<ThreadEpoch>> tls_slots;

        if (auto it = tls_slots.find(this); it != tls_slots.end()) {
            if (auto slot = it->second.lock()) {
                return slot.get();
            }
            tls_slots.erase(it);
        }

        auto  slot = std::make_shared<ThreadEpoch>();
        auto* raw  = slot.get();
        {
            std::lock_guard<std::mutex> lk(registration_mu_);
            registrations_.push_back(slot);
        }
        tls_slots[this] = slot;
        return raw;
    }

    void Reclaim() {
        uint64_t min_epoch = global_epoch_.load(std::memory_order_acquire);
        {
            std::lock_guard<std::mutex> lk(registration_mu_);
            for (const auto& registration : registrations_) {
                if (!registration->active.load(std::memory_order_acquire)) {
                    continue;
                }
                min_epoch =
                    std::min(min_epoch, registration->epoch.load(std::memory_order_acquire));
            }
        }

        std::vector<RetiredNode> reclaimable;
        {
            std::lock_guard<std::mutex> lk(retired_mu_);
            auto                        keep_begin = std::stable_partition(
                retired_.begin(), retired_.end(),
                [min_epoch](const auto& node) { return node.retire_epoch >= min_epoch; });
            reclaimable.assign(keep_begin, retired_.end());
            retired_.erase(keep_begin, retired_.end());
        }

        for (const auto& node : reclaimable) {
            node.deleter(node.ptr);
        }
    }

    void RetireImpl(void* ptr, void (*deleter)(void*)) {
        const uint64_t retire_epoch = global_epoch_.fetch_add(1, std::memory_order_acq_rel) + 1;
        {
            std::lock_guard<std::mutex> lk(retired_mu_);
            retired_.push_back({.retire_epoch = retire_epoch, .ptr = ptr, .deleter = deleter});
        }
        Reclaim();
    }

    mutable std::atomic<uint64_t>                     global_epoch_{1};
    mutable std::mutex                                registration_mu_;
    mutable std::vector<std::shared_ptr<ThreadEpoch>> registrations_;
    mutable std::mutex                                retired_mu_;
    mutable std::vector<RetiredNode>                  retired_;
};

} // namespace idlekv::dash::detail

namespace idlekv::dash {

template <class Key, class Value, class Hash = std::hash<Key>, class KeyEqual = std::equal_to<Key>,
          size_t RegularBuckets = 56, size_t StashBuckets = 4, size_t SlotsPerBucket = 14>
class DashEH {
    static_assert(std::is_copy_constructible_v<Key>,
                  "DashEH currently requires copyable key types");
    static_assert(std::is_copy_constructible_v<Value>,
                  "DashEH currently requires copyable value types");

public:
    using SegmentType = detail::Segment<Key, Value, RegularBuckets, StashBuckets, SlotsPerBucket>;
    using RecordType  = typename SegmentType::RecordType;

    struct Options {
        size_t initial_global_depth = 1;
        double merge_threshold      = 0.20;
    };

    struct Stats {
        uint64_t split_count            = 0;
        uint64_t merge_count            = 0;
        uint64_t directory_growth_count = 0;
        uint64_t directory_shrink_count = 0;
    };

    struct DebugPosition {
        size_t segment_index = 0;
        size_t LocalDepth   = 0;
        size_t bucket        = 0;
        size_t slot          = 0;
        size_t HomeBucket   = 0;
        bool   in_stash      = false;
    };

    explicit DashEH(const Options& options = Options{}, Hash hash = Hash{},
                    KeyEqual eq = KeyEqual{})
        : hash_(std::move(hash)), eq_(std::move(eq)),
          merge_threshold_(NormalizeMergeThreshold(options.merge_threshold)) {
        auto* dir         = new Directory;
        dir->global_depth = options.initial_global_depth;
        dir->segments.resize(size_t{1} << dir->global_depth);
        for (auto& segment : dir->segments) {
            segment = new SegmentType(dir->global_depth);
        }
        directory_.store(dir, std::memory_order_release);
    }

    DashEH(const DashEH&)                    = delete;
    auto operator=(const DashEH&) -> DashEH& = delete;

    ~DashEH() {
        Directory* current = directory_.exchange(nullptr, std::memory_order_acq_rel);
        if (current) {
            DeleteDirectoryWithSegments(current);
        }
        epoch_.DrainAll();
    }

    template <class K, class V>
    auto Insert(K&& key, V&& value) -> bool {
        const Key      key_copy(std::forward<K>(key));
        const Value    value_copy(std::forward<V>(value));
        const uint64_t hash = hash_(key_copy);

        while (true) {
            auto guard = epoch_.Pin();
            (void)guard;
            auto*        dir           = directory_.load(std::memory_order_acquire);
            const size_t segment_index = DirectoryIndex(hash, dir->global_depth);
            auto*        segment       = dir->segments[segment_index];

            auto result = segment->Insert(key_copy, value_copy, hash, eq_);
            if (result.status == SegmentType::InsertStatus::kInserted) {
                size_.fetch_add(1, std::memory_order_acq_rel);
                return true;
            }
            if (result.status == SegmentType::InsertStatus::kDuplicate) {
                return false;
            }
            if (result.status == SegmentType::InsertStatus::kRetry) {
                continue;
            }

            MaybeSplit(dir, segment_index, segment);
        }
    }

    auto Erase(const Key& key) -> bool {
        const uint64_t hash = hash_(key);

        while (true) {
            auto guard = epoch_.Pin();
            (void)guard;
            auto*        dir           = directory_.load(std::memory_order_acquire);
            const size_t segment_index = DirectoryIndex(hash, dir->global_depth);
            auto*        segment       = dir->segments[segment_index];

            auto result = segment->Erase(key, hash, eq_);
            if (result.retry) {
                continue;
            }
            if (!result.erased) {
                return false;
            }

            size_.fetch_sub(1, std::memory_order_acq_rel);
            MaybeMerge(dir, segment_index, segment);
            return true;
        }
    }

    auto Find(const Key& key) const -> std::optional<Value> {
        const uint64_t hash  = hash_(key);
        auto           guard = epoch_.Pin();
        (void)guard;
        auto*        dir           = directory_.load(std::memory_order_acquire);
        const size_t segment_index = DirectoryIndex(hash, dir->global_depth);
        return dir->segments[segment_index]->Find(key, hash, eq_);
    }

    auto FindRecord(const Key& key) const -> std::shared_ptr<const RecordType> {
        const uint64_t hash  = hash_(key);
        auto           guard = epoch_.Pin();
        (void)guard;
        auto*        dir           = directory_.load(std::memory_order_acquire);
        const size_t segment_index = DirectoryIndex(hash, dir->global_depth);
        return dir->segments[segment_index]->FindRecord(key, hash, eq_);
    }

    auto Contains(const Key& key) const -> bool { return Find(key).has_value(); }

    auto Size() const -> size_t { return size_.load(std::memory_order_acquire); }

    static constexpr auto SegmentCapacity() -> size_t { return SegmentType::Capacity(); }

    auto DirectoryDepth() const -> size_t {
        auto* dir = directory_.load(std::memory_order_acquire);
        return dir ? dir->global_depth : 0;
    }

    auto DirectorySize() const -> size_t {
        auto* dir = directory_.load(std::memory_order_acquire);
        return dir ? dir->segments.size() : 0;
    }

    auto UniqueSegments() const -> size_t {
        auto guard = epoch_.Pin();
        (void)guard;
        auto* dir = directory_.load(std::memory_order_acquire);
        if (!dir) {
            return 0;
        }
        std::unordered_set<SegmentType*> unique(dir->segments.begin(), dir->segments.end());
        return unique.size();
    }

    auto Stats() const -> Stats {
        return {
            .split_count            = split_count_.load(std::memory_order_acquire),
            .merge_count            = merge_count_.load(std::memory_order_acquire),
            .directory_growth_count = directory_growth_count_.load(std::memory_order_acquire),
            .directory_shrink_count = directory_shrink_count_.load(std::memory_order_acquire),
        };
    }

    auto DebugLocate(const Key& key) const -> std::optional<DebugPosition> {
        const uint64_t hash  = hash_(key);
        auto           guard = epoch_.Pin();
        (void)guard;
        auto*        dir           = directory_.load(std::memory_order_acquire);
        const size_t segment_index = DirectoryIndex(hash, dir->global_depth);
        auto*        segment       = dir->segments[segment_index];
        auto         location      = segment->Locate(key, hash, eq_);
        if (!location) {
            return std::nullopt;
        }

        return DebugPosition{
            .segment_index = segment_index,
            .LocalDepth   = segment->LocalDepth(),
            .bucket        = location->bucket,
            .slot          = location->slot,
            .HomeBucket   = location->HomeBucket,
            .in_stash      = location->in_stash,
        };
    }

private:
    struct Directory {
        size_t                    global_depth = 0;
        std::vector<SegmentType*> segments;
    };

    static auto NormalizeMergeThreshold(double threshold) -> double {
        return threshold > 0.0 && threshold < 1.0 ? threshold : 0.20;
    }

    static auto DirectoryIndex(uint64_t hash, size_t global_depth) -> size_t {
        if (global_depth == 0) {
            return 0;
        }
        return static_cast<size_t>(hash >> (64U - global_depth));
    }

    static auto PrefixBit(uint64_t hash, size_t depth_before_split) -> uint64_t {
        return (hash >> (63U - depth_before_split)) & 1ULL;
    }

    static auto GrowDirectory(const std::vector<SegmentType*>& current)
        -> std::vector<SegmentType*> {
        std::vector<SegmentType*> expanded(current.size() * 2);
        for (size_t i = 0; i < current.size(); ++i) {
            expanded[2 * i]     = current[i];
            expanded[2 * i + 1] = current[i];
        }
        return expanded;
    }

    static auto ShrinkDirectoryIfPossible(Directory& dir) -> size_t {
        size_t shrink_steps = 0;
        while (dir.global_depth > 0) {
            const size_t half       = size_t{1} << (dir.global_depth - 1);
            bool         can_shrink = true;
            for (size_t i = 0; i < half; ++i) {
                if (dir.segments[i] != dir.segments[i + half]) {
                    can_shrink = false;
                    break;
                }
            }
            if (!can_shrink) {
                break;
            }

            dir.segments.resize(half);
            --dir.global_depth;
            ++shrink_steps;
        }
        return shrink_steps;
    }

    static void DeleteDirectoryWithSegments(Directory* dir) {
        std::unordered_set<SegmentType*> unique(dir->segments.begin(), dir->segments.end());
        for (auto* segment : unique) {
            delete segment;
        }
        delete dir;
    }

    void MaybeSplit(Directory* observed_dir, size_t segment_index, SegmentType* segment) {
        std::lock_guard<std::mutex> lk(structure_mu_);
        auto*                       current = directory_.load(std::memory_order_acquire);
        if (current != observed_dir || current->segments[segment_index] != segment) {
            return;
        }
        if (!segment->TryFreeze()) {
            return;
        }

        bool retire_old_segment = false;
        auto unfreeze           = detail::MakeScopeExit([&] {
            if (!retire_old_segment) {
                segment->Unfreeze();
            }
        });
        (void)unfreeze;

        segment->LockAllBuckets();
        auto unlock = detail::MakeScopeExit([&] { segment->UnlockAllBuckets(); });
        (void)unlock;

        current = directory_.load(std::memory_order_acquire);
        if (current != observed_dir || current->segments[segment_index] != segment) {
            return;
        }

        const auto records = segment->SnapshotRecordsLocked();

        size_t new_depth          = current->global_depth;
        auto   new_segments       = current->segments;
        size_t expanded_seg_index = segment_index;
        bool   grew_directory     = false;
        if (segment->LocalDepth() == new_depth) {
            new_segments = GrowDirectory(new_segments);
            new_depth += 1;
            expanded_seg_index = segment_index * 2;
            grew_directory     = true;
        }

        const size_t chunk = size_t{1} << (new_depth - segment->LocalDepth());
        const size_t start = expanded_seg_index & ~(chunk - 1);
        const size_t half  = chunk / 2;

        std::vector<RecordType> left_records;
        std::vector<RecordType> right_records;
        left_records.reserve(records.size());
        right_records.reserve(records.size());
        for (const auto& record : records) {
            if (PrefixBit(record.hash, segment->LocalDepth()) == 0) {
                left_records.push_back(record);
            } else {
                right_records.push_back(record);
            }
        }

        auto* left                 = new SegmentType(segment->LocalDepth() + 1);
        auto* right                = new SegmentType(segment->LocalDepth() + 1);
        auto  cleanup_new_segments = detail::MakeScopeExit([&] {
            delete left;
            delete right;
        });
        (void)cleanup_new_segments;

        if (!left->RebuildFrom(left_records, eq_) || !right->RebuildFrom(right_records, eq_)) {
            return;
        }

        auto* new_dir =
            new Directory{.global_depth = new_depth, .segments = std::move(new_segments)};
        for (size_t i = start; i < start + half; ++i) {
            new_dir->segments[i] = left;
        }
        for (size_t i = start + half; i < start + chunk; ++i) {
            new_dir->segments[i] = right;
        }

        directory_.store(new_dir, std::memory_order_release);
        epoch_.Retire(observed_dir);
        epoch_.Retire(segment);

        retire_old_segment = true;
        cleanup_new_segments.Release();
        split_count_.fetch_add(1, std::memory_order_acq_rel);
        if (grew_directory) {
            directory_growth_count_.fetch_add(1, std::memory_order_acq_rel);
        }
    }

    void MaybeMerge(Directory* observed_dir, size_t segment_index, SegmentType* segment) {
        if (segment->LocalDepth() == 0 || segment->LoadFactor() > merge_threshold_) {
            return;
        }

        std::lock_guard<std::mutex> lk(structure_mu_);
        auto*                       current = directory_.load(std::memory_order_acquire);
        if (current != observed_dir || current->segments[segment_index] != segment) {
            return;
        }
        if (segment->LocalDepth() == 0) {
            return;
        }

        const size_t chunk       = size_t{1} << (current->global_depth - segment->LocalDepth());
        const size_t start       = segment_index & ~(chunk - 1);
        const size_t buddy_start = start ^ chunk;
        auto*        buddy       = current->segments[buddy_start];
        if (buddy == segment || buddy->LocalDepth() != segment->LocalDepth()) {
            return;
        }

        if (!segment->TryFreeze()) {
            return;
        }
        if (!buddy->TryFreeze()) {
            segment->Unfreeze();
            return;
        }

        bool retire_old_segments = false;
        auto unfreeze_segment    = detail::MakeScopeExit([&] {
            if (!retire_old_segments) {
                segment->Unfreeze();
            }
        });
        (void)unfreeze_segment;
        auto unfreeze_buddy = detail::MakeScopeExit([&] {
            if (!retire_old_segments) {
                buddy->Unfreeze();
            }
        });
        (void)unfreeze_buddy;

        segment->LockAllBuckets();
        buddy->LockAllBuckets();
        auto unlock_segment = detail::MakeScopeExit([&] { segment->UnlockAllBuckets(); });
        auto unlock_buddy   = detail::MakeScopeExit([&] { buddy->UnlockAllBuckets(); });
        (void)unlock_segment;
        (void)unlock_buddy;

        current = directory_.load(std::memory_order_acquire);
        if (current != observed_dir || current->segments[segment_index] != segment ||
            current->segments[buddy_start] != buddy) {
            return;
        }

        auto records       = segment->SnapshotRecordsLocked();
        auto buddy_records = buddy->SnapshotRecordsLocked();
        records.insert(records.end(), buddy_records.begin(), buddy_records.end());

        auto* merged         = new SegmentType(segment->LocalDepth() - 1);
        auto  cleanup_merged = detail::MakeScopeExit([&] { delete merged; });
        (void)cleanup_merged;

        if (!merged->RebuildFrom(records, eq_)) {
            return;
        }

        auto* new_dir =
            new Directory{.global_depth = current->global_depth, .segments = current->segments};
        const size_t combined_start = std::min(start, buddy_start);
        const size_t combined_span  = chunk * 2;
        for (size_t i = combined_start; i < combined_start + combined_span; ++i) {
            new_dir->segments[i] = merged;
        }

        const size_t shrink_steps = ShrinkDirectoryIfPossible(*new_dir);
        directory_.store(new_dir, std::memory_order_release);
        epoch_.Retire(observed_dir);
        epoch_.Retire(segment);
        epoch_.Retire(buddy);

        retire_old_segments = true;
        cleanup_merged.Release();
        merge_count_.fetch_add(1, std::memory_order_acq_rel);
        if (shrink_steps != 0) {
            directory_shrink_count_.fetch_add(shrink_steps, std::memory_order_acq_rel);
        }
    }

    Hash                            hash_;
    KeyEqual                        eq_;
    double                          merge_threshold_;
    detail::EpochManager            epoch_;
    mutable std::atomic<Directory*> directory_{nullptr};
    std::atomic<size_t>             size_{0};
    mutable std::mutex              structure_mu_;
    std::atomic<uint64_t>           split_count_{0};
    std::atomic<uint64_t>           merge_count_{0};
    std::atomic<uint64_t>           directory_growth_count_{0};
    std::atomic<uint64_t>           directory_shrink_count_{0};
};

} // namespace idlekv::dash
