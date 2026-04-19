
#include "db/shard.h"

#include "common/logger.h"
#include "db/transaction.h"
#include "db/txn_queue.h"
#include <memory>

namespace idlekv {

namespace {

TxnId TxnIdGetter(Transaction* txn) { return txn->TxId(); }

} // namespace

Shard::Shard(const Config& cfg, EventLoop* el, mi_heap_t* heap)
    : mr_(heap), queue_(kQueueLen), tx_queue_(TxnIdGetter), id_(el->PoolIndex()) {
    queue_.Start();

    db_slice_.resize(cfg.db_num_);
    for (auto& db : db_slice_) {
        db = std::make_shared<DB>(&mr_);
    }

    Value::InitMr(&mr_);
}

void Shard::PollTransaction(std::shared_ptr<Transaction> caller) {
    bool can_exec_by_myself = caller && caller->IsFreeIn(id_) && caller->GetExecutionPermit(id_);
    bool removed_from_txq = caller && caller->shard_plans_[id_].qp_pos == TxnQueue::InvalidIt;
    if (!can_exec_by_myself && removed_from_txq) {
        return;
    }

    Transaction* cur;
    while (!tx_queue_.Empty()) {
        cur = tx_queue_.Front();

        bool should_run = (cur == caller.get() && can_exec_by_myself) || cur->GetExecutionPermit(id_);
        if (!should_run) {
            break;
        }

        if (cur == caller.get()) {
            can_exec_by_myself = false;
        }

        CHECK_GT(cur->TxId(), commit_txn_id_);
        commit_txn_id_ = cur->TxId();
        cur->ExecCbInShard(this);
    }

    if (can_exec_by_myself) {
        // This is a out of order execute.
        caller->ExecCbInShard(this);
    }
}

} // namespace idlekv
