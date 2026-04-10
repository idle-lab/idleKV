#pragma once

#include "common/config.h"
#include "common/logger.h"
#include "db/storage/art/node.h"

#include <cstddef>
#include <list>
#include <memory_resource>
#include <mimalloc.h>
#include <type_traits>
#include <utility>
namespace idlekv {

#ifndef CACHELINE_SIZE
#define CACHELINE_SIZE 64
#endif

class NodeAlloctor {
public:
    virtual ~NodeAlloctor() = default;
};

// alloc a fix memory block, implement memory reclamation based on EBR
template <class NodeType, size_t PoolSize = 32>
class NodeAlloctorT : public NodeAlloctor {
public:
    explicit NodeAlloctorT(std::pmr::memory_resource* mr) : mr_(mr) {}

    static constexpr size_t Size        = sizeof(NodeType);
    static constexpr size_t Alignment   = alignof(NodeType);
    static constexpr size_t kMaxSlotNum = 64;
    static_assert(Alignment > 0);

    auto New() -> NodeType* {
        void* ptr = mr_->allocate(Size, Alignment);
        return new (ptr) NodeType();

        // void* ptr = nullptr;

        // if (memeory_blocks_.free_list_.empty()) {
        //     Block* block = new (mr_->allocate(sizeof(Block), alignof(Block))) Block();
        //     usage_ += sizeof(Block);
        //     memeory_blocks_.blocks_.push_front(block);
        //     for (uint16_t i = 0; i < kMaxSlotNum; i++) {
        //         memeory_blocks_.free_list_.emplace_back(block, i);
        //     }
        // }

        // std::pair<Block*, uint16_t> idx = memeory_blocks_.free_list_.front();
        // memeory_blocks_.free_list_.pop_front();
        // idx.first->allocd++;
        // ptr = static_cast<void*>(&idx.first->slots[idx.second]);

        // return new (ptr) NodeType();
    }

    auto Free(void* ptr) -> void {
        static_cast<NodeType*>(ptr)->~NodeType();

        mr_->deallocate(ptr, Size, Alignment);
        // Recycle(ptr);
    }

    auto Shrink() -> void {
        // TODO(cyb): recycle unused block.
    }

    auto MemoryUsage() -> size_t { return usage_; }

    ~NodeAlloctorT() {

    }

private:
    auto Recycle(void* ptr) -> void {
        for (auto it = memeory_blocks_.blocks_.begin(); it != memeory_blocks_.blocks_.end(); it++) {
            Block* block = *it;
            auto*  begin = reinterpret_cast<std::byte*>(block->slots);
            auto*  end   = begin + sizeof(block->slots);
            auto*  raw   = static_cast<std::byte*>(ptr);

            if (raw >= begin && raw < end) {
                block->allocd--;
                ptrdiff_t offset = raw - begin;
                CHECK_EQ(offset % static_cast<ptrdiff_t>(sizeof(Slot)), ptrdiff_t{0});
                uint16_t slot_id =
                    static_cast<uint16_t>(offset / static_cast<ptrdiff_t>(sizeof(Slot)));

                memeory_blocks_.free_list_.emplace_back(block, slot_id);
                return;
            }
        }
        UNREACHABLE();
    }

    using Slot = std::aligned_storage_t<Size, Alignment>;
    struct Block {
        Slot     slots[kMaxSlotNum];
        uint16_t allocd{0};
    };

    struct MemoryBlocks {
        std::list<Block*>                      blocks_;
        std::list<std::pair<Block*, uint16_t>> free_list_;
    };
    MemoryBlocks memeory_blocks_;

    size_t free_num_{0}, alloced_{0};
    size_t usage_{0};

    std::pmr::memory_resource* mr_{std::pmr::get_default_resource()};
};

} // namespace idlekv
