
#include "db/shard.h"

#include "common/logger.h"
#include "db/transaction.h"
#include "db/txn_queue.h"

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

void Shard::PollTransaction(Transaction* caller) {
    Transaction* cur;

    while (!tx_queue_.Empty()) {
        cur = tx_queue_.Front();

        if (!cur->IsActivedIn(id_)) {
            break;
        }

        if (cur == caller) {
            caller = nullptr;
        }

        CHECK_GT(cur->TxId(), commit_txn_id_);
        commit_txn_id_ = cur->TxId();
        cur->ExecCbInShard(this);
    }

    if (caller && caller->IsFreeIn(id_)) {
        CHECK_GT(caller->TxId(), commit_txn_id_);
        commit_txn_id_ = caller->TxId();
        caller->ExecCbInShard(this);
    }
}

} // namespace idlekv
