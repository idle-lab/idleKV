#pragma once

#include "common/config.h"
#include "common/logger.h"
#include "db/db.h"
#include "db/txn_queue.h"
#include "server/el_pool.h"
#include "utils/fiber/runtime.h"

#include <atomic>
#include <boost/fiber/buffered_channel.hpp>
#include <boost/fiber/channel_op_status.hpp>
#include <boost/fiber/fiber.hpp>
#include <cstddef>
#include <functional>
#include <memory>
#include <memory_resource>
#include <mimalloc.h>
#include <new>
#include <utility>
#include <vector>
#include <xxhash.h>
namespace idlekv {

using ShardId                     = uint8_t;
constexpr ShardId kInvalidShardId = -1;
constexpr size_t  kQueueLen       = 64;

class TaskQueue {
public:
    using Task = std::function<void()>;

    TaskQueue(size_t capacity) : queue_(capacity) {}

    auto Start() -> void {
        for (int i = 0; i < 1; i++) {
            consumers_.emplace_back(
                LaunchFiber(FiberProps{"TaskQueueConsumer", FiberPriority::HIGH}, [this] {
                    Task task;
                    while (true) {
                        auto res = queue_.pop(task);

                        if (res == boost::fibers::channel_op_status::closed) {
                            return;
                        }

                        task();
                    }
                }));
        }
    }

    template <class Fn>
    auto Add(Fn&& fn) -> void {
        queue_.push(std::forward<Fn>(fn));
    }

    auto Stop() -> void {
        queue_.close();
        for (auto& consumer : consumers_) {
            if (consumer.joinable()) {
                consumer.join();
            }
        }
    }

private:
    std::vector<boost::fibers::fiber> consumers_;

    // a bounded, buffered channel (MPMC queue) suitable to synchronize fibers (running on the same
    // or different threads) via asynchronous message passing.
    // TODO(cyb): lock-free queue.
    boost::fibers::buffered_channel<Task> queue_;
};

class ShardMemoryResource : public std::pmr::memory_resource {
public:
    explicit ShardMemoryResource(mi_heap_t* heap) : heap_(heap) {}

    auto MemoryUsage() const -> size_t { return usage_.load(std::memory_order_relaxed); }
    auto PeakMemoryUsage() const -> size_t { return peak_usage_.load(std::memory_order_relaxed); }

private:
    auto do_allocate(std::size_t size, std::size_t align) -> void* final {
        void* res = mi_heap_malloc_aligned(heap_, size, align);
        if (!res) {
            throw std::bad_alloc{};
        }

        const size_t actual_size = mi_usable_size(res);
        const size_t new_usage =
            usage_.fetch_add(actual_size, std::memory_order_relaxed) + actual_size;
        size_t peak = peak_usage_.load(std::memory_order_relaxed);
        while (peak < new_usage &&
               !peak_usage_.compare_exchange_weak(peak, new_usage, std::memory_order_relaxed)) {
        }

        return res;
    }

    auto do_deallocate(void* ptr, std::size_t size, std::size_t align) -> void final {
        usage_.fetch_sub(mi_usable_size(ptr), std::memory_order_relaxed);

        mi_free_size_aligned(ptr, size, align);
    }

    auto do_is_equal(const std::pmr::memory_resource& o) const noexcept -> bool {
        return this == &o;
    }

    std::atomic_size_t usage_{0};
    std::atomic_size_t peak_usage_{0};
    mi_heap_t*         heap_;
};

// A Data Shared.
class Shard {
public:
    Shard(const Config& cfg, EventLoop* el, mi_heap_t* heap);

    template <class Fn>
    void Add(Fn&& fn) {
        queue_.Add(std::forward<Fn>(fn));
    }

    size_t                     MemoryUsage() const { return mr_.MemoryUsage(); }
    size_t                     PeakMemoryUsage() const { return mr_.PeakMemoryUsage(); }
    std::pmr::memory_resource* MemoryResource() { return &mr_; }
    TxnQueue*                  TxQueue() { return &tx_queue_; }
    TxnId                      CommitedId() const { return commit_txn_id_; }
    void                       CommitTxn(TxnId id) {
        CHECK_GT(id, commit_txn_id_);
        commit_txn_id_ = id;
    }

    void PollTransaction(Transaction* caller);

    DB* DbAt(size_t index) {
        CHECK_LT(index, db_slice_.size());
        return db_slice_[index].get();
    }

    ShardId Id() const { return id_; }

private:
    std::vector<std::shared_ptr<DB>> db_slice_;
    ShardMemoryResource              mr_;

    TaskQueue queue_;
    TxnQueue  tx_queue_;
    TxnId     commit_txn_id_{0};

    ShardId id_;
};

static constexpr uint64_t kSeed = 0x9E3779B97F4A7C15ULL;
inline ShardId            GetShardId(std::string_view s, size_t shard_num) {
    return static_cast<ShardId>(XXH64(s.data(), s.size(), kSeed) % shard_num);
}
inline std::pair<ShardId, KeyFingerprint> ShardIdAndFp(std::string_view s, size_t shard_num) {
    auto hash = XXH64(s.data(), s.size(), kSeed);
    return {static_cast<ShardId>(hash % shard_num), hash};
}

} // namespace idlekv
