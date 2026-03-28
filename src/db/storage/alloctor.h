#pragma once

#include "common/config.h"
#include "server/el_pool.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <functional>
#include <list>
#include <memory_resource>
#include <mimalloc.h>
#include <utility>
#include <vector>
namespace idlekv {

#ifndef CACHELINE_SIZE
#define CACHELINE_SIZE 64
#endif

struct Retired {
    void*                      ptr;
    std::function<void(void*)> deleter;
};

struct alignas(CACHELINE_SIZE) EBRThreadLocal {
    std::atomic<uint64_t>               local_epoch{0};
    std::atomic<bool>                   active{false};
    std::array<std::vector<Retired>, 3> buckets;
    size_t                              retire_count{0};
};

inline thread_local EBRThreadLocal ebr_local{};

class EBRManager {
public:
    auto Init() {
        // threads_.resize(elp->PoolSize());

        // elp->AwaitForeach([this](size_t i, EventLoop* el) {
        //     ebr_local = new EBRThreadLocal;
        //     threads_[i] = ebr_local;
        // });
    }

    inline void Enter() {
        ebr_local.local_epoch.store(global_epoch_.load(std::memory_order_acquire),
                                    std::memory_order_relaxed);
        ebr_local.active.store(true, std::memory_order_release);
    }

    inline void Leave() { ebr_local.active.store(false, std::memory_order_release); }

    inline void Retire(void* ptr, std::function<void(void*)> deleter) {
        uint64_t e = global_epoch_.load(std::memory_order_relaxed);

        ebr_local.buckets[e % 3].emplace_back(ptr, std::move(deleter));
        ebr_local.retire_count++;

        if (ebr_local.retire_count >= kReclaimThreshold) {
            TryAdvanceEpoch();
            Reclaim();
        }
    }

private:
    void TryAdvanceEpoch() {
        uint64_t cur = global_epoch_.load(std::memory_order_acquire);

        for (auto* t : threads_) {
            if (t->active.load(std::memory_order_acquire)) {
                uint64_t le = t->local_epoch.load(std::memory_order_relaxed);
                if (le != cur) {
                    return; // some threads are still in the old epoch
                }
            }
        }

        global_epoch_.fetch_add(1, std::memory_order_acq_rel);
    }

    void Reclaim() {
        uint64_t cur           = global_epoch_.load(std::memory_order_acquire);
        uint64_t reclaim_epoch = (cur + 1) % 3; // E-2

        auto& bucket = ebr_local.buckets[reclaim_epoch];

        for (auto& r : bucket) {
            r.deleter(r.ptr);
        }
        ebr_local.retire_count -= bucket.size();
        bucket.clear();
    }

    std::atomic<uint64_t>        global_epoch_ = 0;
    std::vector<EBRThreadLocal*> threads_;

    static constexpr size_t kReclaimThreshold = 64;
};

class MemoryAlloctor {
public:
    virtual ~MemoryAlloctor() = default;
};

// alloc a fix memory block, implement memory reclamation based on EBR
template <class Type, size_t PoolSize = 32>
class FixMemoryAlloctor : public MemoryAlloctor {
public:
    FixMemoryAlloctor(std::pmr::memory_resource* mr, EBRManager* ebr_mgr)
        : ebr_mgr_(ebr_mgr), mr_(mr) {}
    static constexpr size_t Size        = sizeof(Type);
    static constexpr size_t Alignment   = alignof(Type);
    static constexpr size_t kMaxSlotNum = 256;

    template <class... Args>
    auto New(Args&&... args) -> Type* {
        void* ptr = nullptr;

        if (memeory_blocks_.free_list_.empty()) {
            Block* block = new (mr_->allocate(sizeof(Block), alignof(Block))) Block();
            usage_ += sizeof(Block);
            memeory_blocks_.blocks_.push_front(block);
            for (uint16_t i = 0; i < kMaxSlotNum; i++) {
                memeory_blocks_.free_list_.emplace_back(block, i);
            }
        }

        std::pair<Block*, uint16_t> idx = memeory_blocks_.free_list_.front();
        memeory_blocks_.free_list_.pop_front();
        idx.first->allocd++;
        ptr = static_cast<void*>(idx.first->data + static_cast<ptrdiff_t>(idx.second * Size));

        return new (ptr) Type(std::forward<Args>(args)...);
    }

    auto Free(void* ptr) -> void {
        static_cast<Type*>(ptr)->~Type();
        if (ebr_mgr_ == nullptr) {
            Recycle(ptr);
            return;
        }

        ebr_mgr_->Retire(ptr, [this](void* p) { Recycle(p); });
    }

    auto Shrink() -> void {
        // TODO(cyb): recycle unused block.
    }

    auto MemoryUsage() -> size_t { return usage_; }

private:
    auto Recycle(void* ptr) -> void {
        for (auto it = memeory_blocks_.blocks_.begin(); it != memeory_blocks_.blocks_.end(); it++) {
            Block* block = *it;
            if (ptr >= block->data && ptr < static_cast<void*>(block->data + kMaxSlotNum * Size)) {
                block->allocd--;
                uint8_t slot_id =
                    static_cast<uint8_t>((static_cast<uint8_t*>(ptr) - block->data) / Size);

                memeory_blocks_.free_list_.emplace_back(block, slot_id);
                return;
            }
        }
        UNREACHABLE();
    }

    struct Block {
        uint8_t data[kMaxSlotNum * Size];
        uint8_t allocd{0};
    };

    struct MemoryBlocks {
        std::list<Block*>                      blocks_;
        std::list<std::pair<Block*, uint16_t>> free_list_;
    };
    MemoryBlocks memeory_blocks_;

    size_t free_num_{0}, alloced_{0};
    size_t usage_;

    EBRManager*                ebr_mgr_;
    std::pmr::memory_resource* mr_{std::pmr::get_default_resource()};
};

} // namespace idlekv
