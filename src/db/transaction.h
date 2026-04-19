#pragma once

#include "common/config.h"
#include "common/logger.h"
#include "db/command.h"
#include "db/common.h"
#include "db/engine.h"
#include "db/shard.h"
#include "db/txn_queue.h"
#include "utils/fiber/block_counter.h"
#include "utils/pool/pool.h"

#include <absl/container/inlined_vector.h>
#include <absl/functional/function_ref.h>
#include <atomic>
#include <boost/fiber/barrier.hpp>
#include <boost/fiber/operations.hpp>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <span>
#include <spdlog/fmt/bundled/format.h>
#include <spdlog/spdlog.h>
#include <string>
#include <unordered_set>
#include <vector>

namespace idlekv {

namespace detail {

inline auto EscapeArgForLog(std::string_view arg) -> std::string {
    std::string escaped;
    escaped.reserve(arg.size() * 4 + 2);
    escaped.push_back('"');

    for (unsigned char ch : arg) {
        switch (ch) {
        case '\\':
            escaped += "\\\\";
            break;
        case '"':
            escaped += "\\\"";
            break;
        case '\n':
            escaped += "\\n";
            break;
        case '\r':
            escaped += "\\r";
            break;
        case '\t':
            escaped += "\\t";
            break;
        default:
            if (std::isprint(ch)) {
                escaped.push_back(static_cast<char>(ch));
            } else {
                fmt::format_to(std::back_inserter(escaped), "\\x{:02X}", ch);
            }
            break;
        }
    }

    escaped.push_back('"');
    return escaped;
}

inline auto FormatCommandForLog(const CommandContext& cmdctx) -> std::string {
    const size_t arg_count = cmdctx.args->size();
    std::vector<bool> is_key(arg_count, false);

    for (size_t arg_index : cmdctx.keys.read_keys) {
        if (arg_index < arg_count) {
            is_key[arg_index] = true;
        }
    }
    for (size_t arg_index : cmdctx.keys.write_keys) {
        if (arg_index < arg_count) {
            is_key[arg_index] = true;
        }
    }

    std::string formatted;
    formatted.push_back('[');
    for (size_t i = 0; i < arg_count; ++i) {
        if (i > 0) {
            formatted.push_back(' ');
        }

        std::string_view arg = (*cmdctx.args)[i];
        if (i == 0 || is_key[i]) {
            formatted.append(arg.data(), arg.size());
        } else {
            formatted += EscapeArgForLog(arg);
        }
    }
    formatted.push_back(']');
    return formatted;
}

} // namespace detail

enum class MultiMode : uint8_t {
    Default,
    Squash,
};

class MultiCmd {
public:
    explicit MultiCmd(MultiMode mode = MultiMode::Default) : mode_(mode) {}

    auto Size() -> size_t { return cmds_.size(); }

    auto Append(CommandContext cmdctx) -> void { cmds_.emplace_back(std::move(cmdctx)); }

    auto Mode() -> MultiMode { return mode_; }

    auto Next() -> CommandContext {
        CHECK(!cmds_.empty());
        CommandContext ctx = std::move(cmds_[progress_]);
        progress_++;
        return ctx;
    }

    auto IsFinished() -> bool { return progress_ == cmds_.size(); }

private:
    std::vector<CommandContext> cmds_;
    size_t                      progress_{0};
    MultiMode                   mode_;
};

/*
1. normal: single-command transaction

2. multi:
    - atomic multi-command transaction for MULTI...EXEC

    - multi-command pipeline execution. The entire pipeline does not need to be atomic;
      commands only need to be executed in order.

        A pipeline needs to create multiple MultiSub child transactions
        (depending on how many shards are involved in the current batch of commands),
        and each child transaction creates a MultiCmd in Squash mode.

        A MultiSub child transaction runs directly on the worker fiber of the
        corresponding shard and does not need to be scheduled.
*/
enum class TxnType : uint8_t {
    Uninitialized,
    Normal,
    Multi,
    MultiSub, // for use inside shard
};

class Transaction : public std::enable_shared_from_this<Transaction> {
    friend class Shard;

    using CallBackType = absl::FunctionRef<void(Transaction*, Shard*)>;

public:
    using SPFlag = uint8_t;
    enum FlagEnum : SPFlag {
        // Free and Blocked flag are for VLL algo.
        // Free transactions are immediately executed.
        Free = 1ULL << 0,
        // Blocked transactions cannot execute fully, since not all locks have been acquired.
        // Blocked transactions are not allowed to begin executing until they are explicitly
        // unblocked by the VLL algorithm.
        Blocked = 1ULL << 1,

        // This flag means that we acquired key locks in this shard.
        // This is used for cleaning up.
        LockAcquired = 1ULL << 2,

        OptimizeExecution = 1ULL << 4,
    };

    struct ShardPlan {
        ShardPlan() {
        }
        ShardPlan([[maybe_unused]] ShardPlan&& other) noexcept {
        }
        std::vector<uint32_t>              arg_indices;
        std::unordered_set<KeyFingerprint> read_fps;
        std::unordered_set<KeyFingerprint> write_fps;

        SPFlag flag;

        std::atomic_bool execution_permit = false;

        TxnQueue::Iterator qp_pos = TxnQueue::InvalidIt;
    };

    Transaction() = default;

    DISABLE_COPY_MOVE(Transaction);

    static auto AcquirePooled() -> std::shared_ptr<Transaction>;

    auto ResetForReuse() noexcept -> void;

    auto InitMulti(size_t db_index, MultiMode mode = MultiMode::Default) -> void {
        type_     = TxnType::Multi;
        txn_id_   = 0;
        db_index_ = db_index;
        single_.reset();
        multi_ = std::make_unique<MultiCmd>(mode);
    }

    auto CreateMultiSub(Shard* local_shard) -> std::shared_ptr<Transaction> {
        CHECK_EQ(type_, TxnType::Multi);
        auto sub = AcquirePooled();

        sub->type_              = TxnType::MultiSub;
        sub->multi_             = std::make_unique<MultiCmd>(multi_->Mode());
        sub->unique_shard_      = local_shard;
        sub->active_shard_count = 1;
        sub->db_index_          = db_index_;

        return sub;
    }

    auto CollectCmd(CommandContext cmdctx) -> void {
        CHECK(multi_ != nullptr);
        multi_->Append(std::move(cmdctx));
    }

    auto MultiNext() -> CommandContext {
        auto cmdctx = multi_->Next();
        InitByArgs(*cmdctx.args, cmdctx.keys);
        return cmdctx;
    }

    auto MultiSize() -> size_t {
        CHECK(multi_);
        return multi_->Size();
    }

    auto IsFinished() -> bool { return multi_->IsFinished(); }

    auto InitSingle(Cmd* cmd, CmdArgs* args, WRSet keys, size_t db_index) -> void {
        type_     = TxnType::Normal;
        txn_id_   = 0;
        db_index_ = db_index;
        multi_.reset();
        single_ = std::make_unique<CommandContext>(cmd, args, keys);
        InitByArgs(*single_->args, single_->keys);
    }

    std::span<const uint32_t> GetShardArgs(ShardId id) const {
        CHECK_LT(id, shard_plans_.size());
        return shard_plans_[id].arg_indices;
    }

    void Execute(CallBackType task) {
        CHECK_NE(type_, TxnType::Uninitialized);
        if (multi_ && multi_->Mode() == MultiMode::Squash) {
            task(this, engine->LocalShard());
            return;
        }

        cb_ptr_ = &task;

        RegisterOnActiveShards();

        int run_count = 0;
        // If our arrival here means that we have completed the registration operations on all
        // active shards, mark each shard plan as activated
        IteratorActiveShardPlan([&](ShardPlan& sp) {
            if ((sp.flag & OptimizeExecution) == 0) {
                run_count++;
            }
        });

        if (run_count == 0) {
            return;
        }
        block_counter_.Start(run_count);

        std::atomic_thread_fence(std::memory_order_release);
        IteratorActiveShardPlan([&](ShardPlan& sp) {
            if ((sp.flag & OptimizeExecution) == 0) {
                sp.execution_permit.store(true, std::memory_order_relaxed);
            }
        });

        if (CanInPlace()) {
            engine->LocalShard()->PollTransaction(shared_from_this());
        }

        for (Shard* shard : shards_) {
            if (!shard) {
                continue;
            }

            if ((shard_plans_[shard->Id()].flag & OptimizeExecution) == 0) {
                shard->Add([self = shared_from_this()]() {
                    engine->LocalShard()->PollTransaction(self);
                });
            }
        }
        block_counter_.Wait();

        // LOG(debug, "txn id: {} finished on {} shards", txn_id_, active_shard_count);
        cb_ptr_ = nullptr;
    }

    TxnId TxId() const { return txn_id_; }

private:
    template <class Fn>
    void DispatchOnActiveShards(Fn&& fn) {
        block_counter_.Start(active_shard_count);
        for (Shard* shard : shards_) {
            if (!shard) {
                continue;
            }
            shard->Add([this, fn = std::forward<Fn>(fn)]() mutable {
                fn();
                block_counter_.Done();
            });
        }
        block_counter_.Wait();
    }

    template <class Fn>
    void IteratorActiveShardPlan(Fn&& fn) {
        for (size_t i = 0; i < shard_plans_.size(); i++) {
            if (!shards_[i]) {
                continue;
            }
            fn(shard_plans_[i]);
        }
    }

    auto InitByArgs(const CmdArgs& args, const WRSet& wr_set) -> void {
        shards_.resize(engine->ShardNum());
        shard_plans_.resize(engine->ShardNum());
        active_shard_count = 0;
        unique_shard_      = nullptr;

        for (size_t i = 0; i < shards_.size(); i++) {
            shards_[i] = nullptr;
            shard_plans_[i].arg_indices.clear();
            shard_plans_[i].read_fps.clear();
            shard_plans_[i].write_fps.clear();
            shard_plans_[i].flag   = 0;
            shard_plans_[i].qp_pos = TxnQueue::InvalidIt;
        }

        auto add_key = [&](uint32_t arg_index, bool is_write) {
            std::string_view key = args[arg_index];
            auto [shard_id, fp]  = ShardIdAndFp(key, engine->ShardNum());

            if (!shards_[shard_id]) {
                shards_[shard_id] = engine->ShardAt(shard_id);
                active_shard_count++;
                unique_shard_ = shards_[shard_id];
            }

            auto& plan = shard_plans_[shard_id];
            plan.arg_indices.push_back(arg_index);
            (is_write ? plan.write_fps : plan.read_fps).insert(fp);
        };

        for (auto arg_index : wr_set.read_keys) {
            add_key(arg_index, false);
        }
        for (auto arg_index : wr_set.write_keys) {
            add_key(arg_index, true);
        }
    }

    void RegisterOnActiveShards() {
        // Loop until this txn register on active shards successfully.
        while (true) {
            if (CanInPlace()) {
                // Single-shard transaction should always register successfully.
                // LOG(debug, "single-shard transaction try register, {}", detail::FormatCommandForLog(*single_));
                CHECK(this->RegisterInShard(engine->LocalShard()));
                break;
            }

            if (active_shard_count > 1 && txn_id_ == InvalidTxnId) {
                txn_id_ = engine->NextTxnId();
                // LOG(debug, "multi-shard transactions get txn id: {}, {}", txn_id_, detail::FormatCommandForLog(*single_));
            }

            std::atomic_bool should_retry{false};

            DispatchOnActiveShards([this, &should_retry]() {
                if (!this->RegisterInShard(engine->LocalShard())) {
                    should_retry.store(true, std::memory_order_relaxed);
                }
            });

            if (!should_retry.load(std::memory_order_relaxed)) {
                break;
            }
            LOG(info, "multi-shard txn id: {} register failure, need retry", txn_id_);

            txn_id_ = InvalidTxnId;
            std::atomic_bool should_poll_txn{false};
            DispatchOnActiveShards([this, &should_poll_txn]() {
                auto* shard = engine->LocalShard();
                auto& plan  = shard_plans_[shard->Id()];
                auto* txq   = shard->TxQueue();

                if (plan.flag & LockAcquired) {
                    shard->DbAt(db_index_)->ReleaseTxnLocks(plan.read_fps, plan.write_fps);
                }

                if (plan.qp_pos != TxnQueue::InvalidIt) {
                    if (txq->Front() == this && txq->Size() > 1) {
                        should_poll_txn.store(true, std::memory_order_relaxed);
                    }
                    shard->TxQueue()->Erase(plan.qp_pos);
                    plan.qp_pos = TxnQueue::InvalidIt;
                }

                plan.flag = 0;
            });

            if (should_poll_txn.load(std::memory_order_relaxed)) {
                DispatchOnActiveShards([]() {
                    engine->LocalShard()->PollTransaction(nullptr);
                });
            }
        }

        // LOG(debug, "txn id {} register on all active shard successfully", txn_id_);
    }


    // this function will be executed in target shard.
    bool RegisterInShard(Shard* shard) {
        CHECK(shard != nullptr);
        // LOG(debug, "[{}] try register txn id: {} ", shard->Id(), txn_id_);
        if (txn_id_ != InvalidTxnId && shard->CommitedId() > txn_id_) {
            return false;
        }

        TxnQueue* txq  = shard->TxQueue();
        auto&     plan = shard_plans_[shard->Id()];
        auto*     db   = shard->DbAt(db_index_);

        auto release_lock = [&]() {
            db->ReleaseTxnLocks(plan.read_fps, plan.write_fps);
            plan.flag &= ~LockAcquired;
        };

        bool key_locked = db->AcquireTxnLocks(plan.read_fps, plan.write_fps);
        plan.flag |= (LockAcquired | (key_locked ? Free : Blocked));

        if (key_locked && active_shard_count == 1) {
            (*cb_ptr_)(this, shard);

            plan.flag |= OptimizeExecution;

            release_lock();
            // LOG(debug, "[{}] txn id: {} optimize execution", shard->Id(), txn_id_);
            return true;
        }

        if (txn_id_ == InvalidTxnId) {
            // Deferring TxnId assignment for single-shard transactions to RegisterInShard
            // guarantees that they are registered with the largest TxnId in the current shard,
            // ensuring success.
            CHECK_EQ(active_shard_count, 1);
            txn_id_ = engine->NextTxnId();
            // LOG(debug, "[{}] single-shard transactions get id: {}, {}", shard->Id(), txn_id_, detail::FormatCommandForLog(*single_));
        }

        // If we cannot reorder the current TxnQueue, we can only abort this registration attempt.
        if (!txq->Empty() && txq->Back()->TxId() > txn_id_ && !key_locked) {
            release_lock();
            // LOG(debug, "[{}] txn id: {} cannot reorder current TxnQueue. Abort!", shard->Id(), txn_id_);
            return false;
        }

        plan.qp_pos = txq->Insert(this);
        // LOG(debug, "[{}] txn id: {} register successfully", shard->Id(), txn_id_);
        return true;
    }

    void ExecCbInShard(Shard* shard) {
        (*cb_ptr_)(this, shard);

        auto& plan = shard_plans_[shard->Id()];
        
        CHECK_NE(plan.qp_pos, TxnQueue::InvalidIt);
        shard->TxQueue()->Erase(plan.qp_pos);
        plan.qp_pos = TxnQueue::InvalidIt;

        CHECK(plan.flag & LockAcquired);
        auto* db = shard->DbAt(db_index_);
        db->ReleaseTxnLocks(plan.read_fps, plan.write_fps);
        plan.flag = 0;
        block_counter_.Done();
        // LOG(debug, "[{}] txn id {} exec callback successfully", shard->Id(), txn_id_);
    }

    bool GetExecutionPermit(ShardId sid) {
        return shard_plans_[sid].execution_permit.exchange(false, std::memory_order_release); 
    }

    bool IsFreeIn(ShardId sid) { return shard_plans_[sid].flag & Free; }

    bool CanInPlace() const { return active_shard_count == 1 && unique_shard_ == engine->LocalShard(); }

    // Only assigned to transactions that span multiple shards and need a global order.
    TxnId   txn_id_{InvalidTxnId};
    TxnType type_{TxnType::Uninitialized};

    utils::SingleWaiterBlockCounter block_counter_;
    CallBackType*                   cb_ptr_;

    absl::InlinedVector<Shard*, 6>    shards_;
    absl::InlinedVector<ShardPlan, 6> shard_plans_;
    size_t                            active_shard_count{0};
    // only valid when active_shard_count == 1
    Shard* unique_shard_{nullptr};
    size_t db_index_{0};

    std::unique_ptr<MultiCmd>       multi_;
    std::unique_ptr<CommandContext> single_;
};

namespace detail {

inline auto ThreadLocalTxnPool() -> utils::Pool<std::unique_ptr<Transaction>>& {
    constexpr size_t kTxnPoolSize = 256;
    thread_local utils::Pool<std::unique_ptr<Transaction>> pool{
        kTxnPoolSize, []() { return std::make_unique<Transaction>(); }};
    return pool;
}

} // namespace detail

inline auto Transaction::AcquirePooled() -> std::shared_ptr<Transaction> {
    auto holder = detail::ThreadLocalTxnPool().Get();
    auto* txn   = holder.release();
    txn->ResetForReuse();

    return std::shared_ptr<Transaction>(txn, [](Transaction* raw) {
        raw->ResetForReuse();
        detail::ThreadLocalTxnPool().Put(std::unique_ptr<Transaction>(raw));
    });
}

inline auto Transaction::ResetForReuse() noexcept -> void {
    txn_id_             = InvalidTxnId;
    type_               = TxnType::Uninitialized;
    cb_ptr_             = nullptr;
    active_shard_count  = 0;
    unique_shard_       = nullptr;
    db_index_           = 0;

    multi_.reset();
    single_.reset();

    for (auto& plan : shard_plans_) {
        plan.arg_indices.clear();
        plan.read_fps.clear();
        plan.write_fps.clear();
        plan.flag = 0;
        plan.execution_permit.store(false, std::memory_order_relaxed);
        plan.qp_pos = TxnQueue::InvalidIt;
    }

    shard_plans_.clear();
    shards_.clear();
}

} // namespace idlekv
