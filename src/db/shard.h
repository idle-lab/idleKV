#pragma once

#include "common/config.h"
#include "db/db.h"
#include "db/storage/alloctor.h"
#include "server/el_pool.h"

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
namespace idlekv {

using ShardId              = uint8_t;
constexpr size_t kQueueLen = 64;

class TaskQueue {
public:
    using Task = std::function<void()>;

    TaskQueue(size_t capacity) : queue_(capacity) {}

    auto Start() -> void {
        worker_fiber_ = boost::fibers::fiber([this] {
            Task task;
            while (true) {
                auto res = queue_.pop(task);

                if (res == boost::fibers::channel_op_status::closed) {
                    return;
                }

                task();
            }
        });
        worker_fiber_.detach();
    }

    template <class Fn>
    auto Add(Fn&& fn) -> void {
        queue_.push(std::forward<Fn>(fn));
    }

    auto Stop() -> void { 
        queue_.close();
    }

private:
    boost::fibers::fiber worker_fiber_;

    // a bounded, buffered channel (MPMC queue) suitable to synchronize fibers (running on the same
    // or different threads) via asynchronous message passing.
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
    }

    template <class Fn>
    auto Add(Fn&& fn) -> void {
        queue_.Add(std::forward<Fn>(fn));
    }

    auto MemoryUsage() -> size_t { return mr_.MemoryUsage(); }

    auto DbAt(size_t index) -> DB* {
        CHECK_LT(index, db_slice_.size());
        return db_slice_[index].get();
    }

private:
    std::vector<std::shared_ptr<DB>> db_slice_;
    ShardMemoryResource              mr_;

    EBRManager ebr_mgr_; // TODO(cyb): plan to abolish

    TaskQueue queue_;

    ShardId id_;
};

} // namespace idlekv