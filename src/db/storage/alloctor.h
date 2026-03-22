#pragma once


#include "server/el_pool.h"
#include <array>
#include <atomic>
#include <cstddef>
#include <functional>
#include <memory_resource>
#include <mimalloc.h>
#include <new>
#include <utility>
#include <vector>
namespace idlekv {

#ifndef CACHELINE_SIZE
#define CACHELINE_SIZE 64
#endif

struct Retired {
    void* ptr;
    std::function<void(void*)> deleter;
};


struct alignas(CACHELINE_SIZE) EBRThreadLocal {
    std::atomic<uint64_t> local_epoch{0};
    std::atomic<bool>     active{false};
    std::array<std::vector<Retired>, 3> buckets;
    size_t retire_count{0};
};

extern thread_local EBRThreadLocal* ebr_local;

class EBRManager {
public:
    auto Init(EventLoopPool* elp) {
        threads_.resize(elp->PoolSize());

        elp->AwaitForeach([this](size_t i, EventLoop* el) {
            ebr_local = new EBRThreadLocal;
            threads_[i] = ebr_local;
        });
    }

    inline void Enter() {
        ebr_local->local_epoch.store(global_epoch_.load(std::memory_order_acquire),
                              std::memory_order_relaxed);
        ebr_local->active.store(true, std::memory_order_release);
    }

    inline void Leave() {
        ebr_local->active.store(false, std::memory_order_release);
    }

    inline void Retire(void* ptr, std::function<void(void*)> deleter) {
        uint64_t e = global_epoch_.load(std::memory_order_relaxed);

        ebr_local->buckets[e % 3].emplace_back(ptr, std::move(deleter));
        ebr_local->retire_count++;

        if (ebr_local->retire_count >= kReclaimThreshold) {
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
        uint64_t cur = global_epoch_.load(std::memory_order_acquire);
        uint64_t reclaim_epoch = (cur + 1) % 3; // E-2

        auto& bucket = ebr_local->buckets[reclaim_epoch];

        for (auto& r : bucket) {
            r.deleter(r.ptr);
        }
        ebr_local->retire_count -= bucket.size();
        bucket.clear();
    }

    std::atomic<uint64_t> global_epoch_ = 0;
    std::vector<EBRThreadLocal*> threads_;

    static constexpr size_t kReclaimThreshold = 64;
};


class MemoryAlloctor {
public:
    virtual ~MemoryAlloctor() = default;
};

// alloc a fix memory block, implement memory reclamation based on EBR
template<class Type, size_t PoolSize = 32>
class FixMemoryAlloctor : public MemoryAlloctor {
public:
    explicit FixMemoryAlloctor() : ebr_mgr_(nullptr) {}
    FixMemoryAlloctor(EBRManager* ebr_mgr) : ebr_mgr_(ebr_mgr) {}
    static constexpr size_t Size = sizeof(Type);
    static constexpr size_t Alignment = alignof(Type);

    template<class ...Args>
    auto New(Args&&... args) -> Type* {
        if (free_num_ > 0) {
            void* ptr = free_memory_blocks_[--free_num_];
            return new (ptr) Type(std::forward<Args>(args)...);
        }

        void* ptr = mi_malloc_aligned(Size, Alignment);
        if (!ptr) {
            throw std::bad_alloc{};
        }

        usage_ += mi_usable_size(ptr);

        alloced_++;
        return new (ptr) Type(std::forward<Args>(args)...);
    }

    auto Free(void* ptr) -> void {
        static_cast<Type*>(ptr)->~Type();
        if (ebr_mgr_ == nullptr) {
            if (free_num_ < PoolSize) {
                free_memory_blocks_[free_num_++] = ptr;
            } else {
                usage_ -= mi_usable_size(ptr);
                mi_free_size_aligned(ptr, Size, Alignment);
            }
            return;
        }

        ebr_mgr_->Retire(ptr, [this](void* p) {
            if (free_num_ < PoolSize) {
                free_memory_blocks_[free_num_++] = p;
                return;
            }
            usage_ -= mi_usable_size(p);
            mi_free_size_aligned(p, Size, Alignment);
        });
    }

    auto Shrink() -> void {
        for (int i = 0; i< free_num_; i++) {
            mi_free_size_aligned(free_memory_blocks_[i], Size, Alignment);
        }
        free_num_ = 0;
    }

    auto MemoryUsage() -> size_t { return usage_; }

private:
    std::array<void*, PoolSize> free_memory_blocks_;
    size_t free_num_{0}, alloced_{0};
    size_t usage_;

    EBRManager* ebr_mgr_;
};



} // namespace idlekv
