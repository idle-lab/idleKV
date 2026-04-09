#pragma once

#include "common/config.h"
#include "db/db.h"
#include "db/storage/alloctor.h"
#include "db/storage/value.h"
#include "server/el_pool.h"
#include "utils/fiber/runtime.h"

#include <boost/fiber/buffered_channel.hpp>
#include <boost/fiber/channel_op_status.hpp>
#include <boost/fiber/fiber.hpp>
#include <cstddef>
#include <functional>
#include <memory>
#include <memory_resource>
#include <mimalloc.h>
#include <new>
#include <vector>
namespace idlekv {

using ShardId                     = uint8_t;
constexpr ShardId kInvalidShardId = -1;
constexpr size_t  kQueueLen       = 64;

// class TaskQueue {
// public:
//     using Task = std::function<void()>;

//     TaskQueue(size_t capacity) : queue_(capacity) {}

//     auto Start() -> void {
//         worker_fiber_ = boost::fibers::fiber([this] {
//             Task task;
//             while (WaitForTask(task)) {
//                 task();
//                 push_ec_.notify();
//             }
//         });
//     }

//     template <class Fn>
//     auto Add(Fn&& fn) -> void {
//         Task task(std::forward<Fn>(fn));
//         if (!EnqueueTask(task)) {
//             return;
//         }

//         pull_ec_.notify();
//     }

//     auto Stop() -> void {
//         stop_.store(true, std::memory_order_release);
//         push_ec_.notifyAll();
//         pull_ec_.notifyAll();
//         if (worker_fiber_.joinable()) {
//             worker_fiber_.join();
//         }
//     }

// private:
//     auto WaitForTask(Task& task) -> bool {
//         while (true) {
//             if (queue_.try_dequeue(task)) {
//                 return true;
//             }

//             if (stop_.load(std::memory_order_acquire)) {
//                 return false;
//             }

//             auto key = pull_ec_.prepareWait();
//             if (queue_.try_dequeue(task)) {
//                 return true;
//             }

//             if (stop_.load(std::memory_order_acquire)) {
//                 return false;
//             }

//             pull_ec_.wait(key.epoch());
//         }
//     }

//     auto EnqueueTask(Task& task) -> bool {
//         while (true) {
//             if (stop_.load(std::memory_order_acquire)) {
//                 return false;
//             }

//             if (queue_.try_enqueue(std::move(task))) {
//                 return true;
//             }

//             auto key = push_ec_.prepareWait();
//             if (stop_.load(std::memory_order_acquire)) {
//                 return false;
//             }

//             if (queue_.try_enqueue(std::move(task))) {
//                 return true;
//             }

//             push_ec_.wait(key.epoch());
//         }
//     }

// private:
//     boost::fibers::fiber worker_fiber_;
//     std::atomic_bool     stop_{false};

//     utils::MpmcBoundedQueue<Task> queue_;

//     // `pull_ec_` wakes the consumer when producers make the queue non-empty.
//     utils::EventCount pull_ec_;

//     // `push_ec_` wakes blocked producers when the consumer frees a slot.
//     utils::EventCount push_ec_;
// };

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

    auto MemoryUsage() -> size_t { return usage_; }

private:
    auto do_allocate(std::size_t size, std::size_t align) -> void* final {
        void* res = mi_heap_malloc_aligned(heap_, size, align);
        if (!res) {
            throw std::bad_alloc{};
        }

        usage_ += mi_usable_size(res);

        return res;
    }

    auto do_deallocate(void* ptr, std::size_t size, std::size_t align) -> void final {
        usage_ -= mi_usable_size(ptr);

        mi_free_size_aligned(ptr, size, align);
    }

    auto do_is_equal(const std::pmr::memory_resource& o) const noexcept -> bool {
        return this == &o;
    }

    size_t     usage_{0};
    mi_heap_t* heap_;
};

// A Data Shared.
class Shard {
public:
    Shard(const Config& cfg, EventLoop* el, mi_heap_t* heap)
        : mr_(heap), queue_(kQueueLen), id_(el->PoolIndex()) {
        queue_.Start();

        db_slice_.resize(cfg.db_num_);
        for (auto& db : db_slice_) {
            db = std::make_shared<DB>(&mr_);
        }

        Value::InitMr(&mr_);
    }

    template <class Fn>
    auto Add(Fn&& fn) -> void {
        queue_.Add(std::forward<Fn>(fn));
    }

    auto MemoryUsage() -> size_t { return mr_.MemoryUsage(); }
    auto MemoryResource() -> std::pmr::memory_resource* { return &mr_; }

    auto DbAt(size_t index) -> DB* {
        CHECK_LT(index, db_slice_.size());
        return db_slice_[index].get();
    }

    auto GetShardId() const -> ShardId { return id_; }

private:
    std::vector<std::shared_ptr<DB>> db_slice_;
    ShardMemoryResource              mr_;

    EBRManager ebr_mgr_; // TODO(cyb): plan to abolish

    TaskQueue queue_;

    ShardId id_;
};

static constexpr uint64_t kSeed = 0x9E3779B97F4A7C15ULL;
inline auto               CalculateShardId(std::string_view s, size_t shard_num) -> ShardId {
    return static_cast<ShardId>(XXH64(s.data(), s.size(), kSeed) % shard_num);
}

} // namespace idlekv
