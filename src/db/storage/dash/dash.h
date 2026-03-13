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

    void release() { active_ = false; }

private:
    Fn   fn_;
    bool active_ = true;
};

template <class Fn>
auto make_scope_exit(Fn&& fn) -> ScopeExit<std::decay_t<Fn>> {
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
            reset();
            slot_       = other.slot_;
            other.slot_ = nullptr;
            return *this;
        }

        Guard(const Guard&)                    = delete;
        auto operator=(const Guard&) -> Guard& = delete;

        ~Guard() { reset(); }

        void reset() {
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

    auto pin() const -> Guard {
        return Guard(get_or_register_slot(), global_epoch_.load(std::memory_order_acquire));
    }

    template <class T>
    void retire(T* ptr) {
        retire_impl(ptr, [](void* raw) { delete static_cast<T*>(raw); });
    }

    void drain_all() {
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
        uint64_t      retire_epoch = 0;
        void*         ptr          = nullptr;
        void (*deleter)(void*)     = nullptr;
    };

    using ThreadEpoch = Guard::ThreadEpoch;

    auto get_or_register_slot() const -> ThreadEpoch* {
        thread_local std::unordered_map<const EpochManager*, ThreadEpoch*> tls_slots;

        if (auto it = tls_slots.find(this); it != tls_slots.end()) {
            return it->second;
        }

        auto slot = std::make_unique<ThreadEpoch>();
        auto* raw = slot.get();
        {
            std::lock_guard<std::mutex> lk(registration_mu_);
            registrations_.push_back(std::move(slot));
        }
        tls_slots.emplace(this, raw);
        return raw;
    }

    void reclaim() {
        uint64_t min_epoch = global_epoch_.load(std::memory_order_acquire);
        {
            std::lock_guard<std::mutex> lk(registration_mu_);
            for (const auto& registration : registrations_) {
                if (!registration->active.load(std::memory_order_acquire)) {
                    continue;
                }
                min_epoch = std::min(min_epoch,
                                     registration->epoch.load(std::memory_order_acquire));
            }
        }

        std::vector<RetiredNode> reclaimable;
        {
            std::lock_guard<std::mutex> lk(retired_mu_);
            auto keep_begin =
                std::stable_partition(retired_.begin(), retired_.end(), [min_epoch](const auto& node) {
                    return node.retire_epoch >= min_epoch;
                });
            reclaimable.assign(keep_begin, retired_.end());
            retired_.erase(keep_begin, retired_.end());
        }

        for (const auto& node : reclaimable) {
            node.deleter(node.ptr);
        }
    }

    void retire_impl(void* ptr, void (*deleter)(void*)) {
        const uint64_t retire_epoch = global_epoch_.fetch_add(1, std::memory_order_acq_rel) + 1;
        {
            std::lock_guard<std::mutex> lk(retired_mu_);
            retired_.push_back({.retire_epoch = retire_epoch, .ptr = ptr, .deleter = deleter});
        }
        reclaim();
    }

    mutable std::atomic<uint64_t>                    global_epoch_{1};
    mutable std::mutex                               registration_mu_;
    mutable std::vector<std::unique_ptr<ThreadEpoch>> registrations_;
    mutable std::mutex                               retired_mu_;
    mutable std::vector<RetiredNode>                 retired_;
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
        size_t local_depth   = 0;
        size_t bucket        = 0;
        size_t slot          = 0;
        size_t home_bucket   = 0;
        bool   in_stash      = false;
    };

    explicit DashEH(const Options& options = Options{}, Hash hash = Hash{},
                    KeyEqual eq = KeyEqual{})
        : hash_(std::move(hash)),
          eq_(std::move(eq)),
          merge_threshold_(normalize_merge_threshold(options.merge_threshold)) {
        auto* dir = new Directory;
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
            delete_directory_with_segments(current);
        }
        epoch_.drain_all();
    }

    template <class K, class V>
    auto insert(K&& key, V&& value) -> bool {
        const Key      key_copy(std::forward<K>(key));
        const Value    value_copy(std::forward<V>(value));
        const uint64_t hash = hash_(key_copy);

        while (true) {
            auto guard = epoch_.pin();
            (void)guard;
            auto* dir  = directory_.load(std::memory_order_acquire);
            const size_t segment_index = directory_index(hash, dir->global_depth);
            auto*        segment       = dir->segments[segment_index];

            auto result = segment->insert(key_copy, value_copy, hash, eq_);
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

            maybe_split(dir, segment_index, segment);
        }
    }

    auto erase(const Key& key) -> bool {
        const uint64_t hash = hash_(key);

        while (true) {
            auto guard = epoch_.pin();
            (void)guard;
            auto* dir  = directory_.load(std::memory_order_acquire);
            const size_t segment_index = directory_index(hash, dir->global_depth);
            auto*        segment       = dir->segments[segment_index];

            auto result = segment->erase(key, hash, eq_);
            if (result.retry) {
                continue;
            }
            if (!result.erased) {
                return false;
            }

            size_.fetch_sub(1, std::memory_order_acq_rel);
            maybe_merge(dir, segment_index, segment);
            return true;
        }
    }

    auto find(const Key& key) const -> std::optional<Value> {
        const uint64_t hash = hash_(key);
        auto           guard = epoch_.pin();
        (void)guard;
        auto*          dir   = directory_.load(std::memory_order_acquire);
        const size_t   segment_index = directory_index(hash, dir->global_depth);
        return dir->segments[segment_index]->find(key, hash, eq_);
    }

    auto contains(const Key& key) const -> bool {
        return find(key).has_value();
    }

    auto size() const -> size_t { return size_.load(std::memory_order_acquire); }

    static constexpr auto segment_capacity() -> size_t { return SegmentType::capacity(); }

    auto directory_depth() const -> size_t {
        auto* dir = directory_.load(std::memory_order_acquire);
        return dir ? dir->global_depth : 0;
    }

    auto directory_size() const -> size_t {
        auto* dir = directory_.load(std::memory_order_acquire);
        return dir ? dir->segments.size() : 0;
    }

    auto unique_segments() const -> size_t {
        auto guard = epoch_.pin();
        (void)guard;
        auto* dir  = directory_.load(std::memory_order_acquire);
        if (!dir) {
            return 0;
        }
        std::unordered_set<SegmentType*> unique(dir->segments.begin(), dir->segments.end());
        return unique.size();
    }

    auto stats() const -> Stats {
        return {
            .split_count            = split_count_.load(std::memory_order_acquire),
            .merge_count            = merge_count_.load(std::memory_order_acquire),
            .directory_growth_count = directory_growth_count_.load(std::memory_order_acquire),
            .directory_shrink_count = directory_shrink_count_.load(std::memory_order_acquire),
        };
    }

    auto debug_locate(const Key& key) const -> std::optional<DebugPosition> {
        const uint64_t hash = hash_(key);
        auto           guard = epoch_.pin();
        (void)guard;
        auto*          dir   = directory_.load(std::memory_order_acquire);
        const size_t   segment_index = directory_index(hash, dir->global_depth);
        auto*          segment       = dir->segments[segment_index];
        auto           location      = segment->locate(key, hash, eq_);
        if (!location) {
            return std::nullopt;
        }

        return DebugPosition{
            .segment_index = segment_index,
            .local_depth   = segment->local_depth(),
            .bucket        = location->bucket,
            .slot          = location->slot,
            .home_bucket   = location->home_bucket,
            .in_stash      = location->in_stash,
        };
    }

private:
    struct Directory {
        size_t                   global_depth = 0;
        std::vector<SegmentType*> segments;
    };

    static auto normalize_merge_threshold(double threshold) -> double {
        return threshold > 0.0 && threshold < 1.0 ? threshold : 0.20;
    }

    static auto directory_index(uint64_t hash, size_t global_depth) -> size_t {
        if (global_depth == 0) {
            return 0;
        }
        return static_cast<size_t>(hash >> (64U - global_depth));
    }

    static auto prefix_bit(uint64_t hash, size_t depth_before_split) -> uint64_t {
        return (hash >> (63U - depth_before_split)) & 1ULL;
    }

    static auto grow_directory(const std::vector<SegmentType*>& current)
        -> std::vector<SegmentType*> {
        std::vector<SegmentType*> expanded(current.size() * 2);
        for (size_t i = 0; i < current.size(); ++i) {
            expanded[2 * i]     = current[i];
            expanded[2 * i + 1] = current[i];
        }
        return expanded;
    }

    static auto shrink_directory_if_possible(Directory& dir) -> size_t {
        size_t shrink_steps = 0;
        while (dir.global_depth > 0) {
            const size_t half = size_t{1} << (dir.global_depth - 1);
            bool can_shrink = true;
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

    static void delete_directory_with_segments(Directory* dir) {
        std::unordered_set<SegmentType*> unique(dir->segments.begin(), dir->segments.end());
        for (auto* segment : unique) {
            delete segment;
        }
        delete dir;
    }

    void maybe_split(Directory* observed_dir, size_t segment_index, SegmentType* segment) {
        std::lock_guard<std::mutex> lk(structure_mu_);
        auto* current = directory_.load(std::memory_order_acquire);
        if (current != observed_dir || current->segments[segment_index] != segment) {
            return;
        }
        if (!segment->try_freeze()) {
            return;
        }

        bool retire_old_segment = false;
        auto unfreeze = detail::make_scope_exit([&] {
            if (!retire_old_segment) {
                segment->unfreeze();
            }
        });
        (void)unfreeze;

        segment->lock_all_buckets();
        auto unlock = detail::make_scope_exit([&] { segment->unlock_all_buckets(); });
        (void)unlock;

        current = directory_.load(std::memory_order_acquire);
        if (current != observed_dir || current->segments[segment_index] != segment) {
            return;
        }

        const auto records = segment->snapshot_records_locked();

        size_t new_depth          = current->global_depth;
        auto   new_segments       = current->segments;
        size_t expanded_seg_index = segment_index;
        bool   grew_directory     = false;
        if (segment->local_depth() == new_depth) {
            new_segments       = grow_directory(new_segments);
            new_depth         += 1;
            expanded_seg_index = segment_index * 2;
            grew_directory     = true;
        }

        const size_t chunk = size_t{1} << (new_depth - segment->local_depth());
        const size_t start = expanded_seg_index & ~(chunk - 1);
        const size_t half  = chunk / 2;

        std::vector<RecordType> left_records;
        std::vector<RecordType> right_records;
        left_records.reserve(records.size());
        right_records.reserve(records.size());
        for (const auto& record : records) {
            if (prefix_bit(record.hash, segment->local_depth()) == 0) {
                left_records.push_back(record);
            } else {
                right_records.push_back(record);
            }
        }

        auto* left  = new SegmentType(segment->local_depth() + 1);
        auto* right = new SegmentType(segment->local_depth() + 1);
        auto cleanup_new_segments = detail::make_scope_exit([&] {
            delete left;
            delete right;
        });
        (void)cleanup_new_segments;

        if (!left->rebuild_from(left_records, eq_) || !right->rebuild_from(right_records, eq_)) {
            return;
        }

        auto* new_dir = new Directory{.global_depth = new_depth, .segments = std::move(new_segments)};
        for (size_t i = start; i < start + half; ++i) {
            new_dir->segments[i] = left;
        }
        for (size_t i = start + half; i < start + chunk; ++i) {
            new_dir->segments[i] = right;
        }

        directory_.store(new_dir, std::memory_order_release);
        epoch_.retire(observed_dir);
        epoch_.retire(segment);

        retire_old_segment = true;
        cleanup_new_segments.release();
        split_count_.fetch_add(1, std::memory_order_acq_rel);
        if (grew_directory) {
            directory_growth_count_.fetch_add(1, std::memory_order_acq_rel);
        }
    }

    void maybe_merge(Directory* observed_dir, size_t segment_index, SegmentType* segment) {
        if (segment->local_depth() == 0 || segment->load_factor() > merge_threshold_) {
            return;
        }

        std::lock_guard<std::mutex> lk(structure_mu_);
        auto* current = directory_.load(std::memory_order_acquire);
        if (current != observed_dir || current->segments[segment_index] != segment) {
            return;
        }
        if (segment->local_depth() == 0) {
            return;
        }

        const size_t chunk = size_t{1} << (current->global_depth - segment->local_depth());
        const size_t start = segment_index & ~(chunk - 1);
        const size_t buddy_start = start ^ chunk;
        auto*        buddy       = current->segments[buddy_start];
        if (buddy == segment || buddy->local_depth() != segment->local_depth()) {
            return;
        }

        if (!segment->try_freeze()) {
            return;
        }
        if (!buddy->try_freeze()) {
            segment->unfreeze();
            return;
        }

        bool retire_old_segments = false;
        auto unfreeze_segment = detail::make_scope_exit([&] {
            if (!retire_old_segments) {
                segment->unfreeze();
            }
        });
        (void)unfreeze_segment;
        auto unfreeze_buddy = detail::make_scope_exit([&] {
            if (!retire_old_segments) {
                buddy->unfreeze();
            }
        });
        (void)unfreeze_buddy;

        segment->lock_all_buckets();
        buddy->lock_all_buckets();
        auto unlock_segment = detail::make_scope_exit([&] { segment->unlock_all_buckets(); });
        auto unlock_buddy   = detail::make_scope_exit([&] { buddy->unlock_all_buckets(); });
        (void)unlock_segment;
        (void)unlock_buddy;

        current = directory_.load(std::memory_order_acquire);
        if (current != observed_dir || current->segments[segment_index] != segment ||
            current->segments[buddy_start] != buddy) {
            return;
        }

        auto records = segment->snapshot_records_locked();
        auto buddy_records = buddy->snapshot_records_locked();
        records.insert(records.end(), buddy_records.begin(), buddy_records.end());

        auto* merged = new SegmentType(segment->local_depth() - 1);
        auto cleanup_merged =
            detail::make_scope_exit([&] { delete merged; });
        (void)cleanup_merged;

        if (!merged->rebuild_from(records, eq_)) {
            return;
        }

        auto* new_dir =
            new Directory{.global_depth = current->global_depth, .segments = current->segments};
        const size_t combined_start = std::min(start, buddy_start);
        const size_t combined_span  = chunk * 2;
        for (size_t i = combined_start; i < combined_start + combined_span; ++i) {
            new_dir->segments[i] = merged;
        }

        const size_t shrink_steps = shrink_directory_if_possible(*new_dir);
        directory_.store(new_dir, std::memory_order_release);
        epoch_.retire(observed_dir);
        epoch_.retire(segment);
        epoch_.retire(buddy);

        retire_old_segments = true;
        cleanup_merged.release();
        merge_count_.fetch_add(1, std::memory_order_acq_rel);
        if (shrink_steps != 0) {
            directory_shrink_count_.fetch_add(shrink_steps, std::memory_order_acq_rel);
        }
    }

    Hash                                  hash_;
    KeyEqual                              eq_;
    double                                merge_threshold_;
    detail::EpochManager                  epoch_;
    mutable std::atomic<Directory*>       directory_{nullptr};
    std::atomic<size_t>                   size_{0};
    mutable std::mutex                    structure_mu_;
    std::atomic<uint64_t>                 split_count_{0};
    std::atomic<uint64_t>                 merge_count_{0};
    std::atomic<uint64_t>                 directory_growth_count_{0};
    std::atomic<uint64_t>                 directory_shrink_count_{0};
};

} // namespace idlekv::dash
