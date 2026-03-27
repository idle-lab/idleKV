#pragma once

#include "db/db.h"
#include "db/storage/alloctor.h"
#include "server/el_pool.h"

#include <boost/fiber/buffered_channel.hpp>
#include <boost/fiber/fiber.hpp>
#include <cstddef>
#include <functional>
#include <memory>
#include <utility>
#include <vector>
namespace idlekv {

using ShardId = uint8_t;
constexpr size_t kQueueLen = 64;

class TaskQueue {
public:
    using Task = std::function<void()>;

    TaskQueue(size_t capacity) : queue_(capacity) {}

    auto Start() -> void {
        worker_fiber_ = boost::fibers::fiber([this]{
            Task task;
            while (!stop_) {
                queue_.pop(task);

                if (stop_) {
                    return;
                }

                task();
            }
        });
        worker_fiber_.detach();
    }

    template<class Fn>
    auto Add(Fn&& fn) -> void {
        queue_.push(std::forward<Fn>(fn));
    }

    auto Stop() -> void {
        stop_ = true;
    }

private:
    boost::fibers::fiber worker_fiber_;

    // a bounded, buffered channel (MPMC queue) suitable to synchronize fibers (running on the same
    // or different threads) via asynchronous message passing.
    boost::fibers::buffered_channel<Task> queue_;

    bool stop_{false};
};


// A Data Shared.
class Shard {
public:
    Shard(EventLoop* el, mi_heap_t* heap) : heap_(heap), queue_(kQueueLen), id_(el->PoolIndex()) {
        
        queue_.Start();
    }



    auto DbAt(size_t index) -> DB* {
        CHECK_LT(index, db_slice_.size());
        return db_slice_[index].get();
    }

private:
    std::vector<std::shared_ptr<DB>> db_slice_;
    mi_heap_t* heap_;

    EBRManager ebr_mgr_; // TODO(cyb): plan to abolish

    TaskQueue queue_;

    ShardId id_;
};

} // namespace idlekv