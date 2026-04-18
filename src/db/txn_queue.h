#pragma once

#include "db/common.h"

#include <absl/functional/function_ref.h>
#include <algorithm>
#include <cstddef>
#include <list>

namespace idlekv {

class Transaction;

class TxnQueue {
public:
    using TxnList  = std::list<Transaction*>;
    using Iterator = TxnList::iterator;

    inline static const Iterator InvalidIt{nullptr};

    TxnQueue(absl::FunctionRef<TxnId(Transaction*)> id_getter) : id_getter_(id_getter) {}

    Transaction* Front() { return txns_.front(); }
    Transaction* Back() { return txns_.back(); }

    Iterator Insert(Transaction* txn) {
        auto pos = std::find_if(txns_.begin(), txns_.end(), [&](Transaction* queued_txn) {
            return id_getter_(queued_txn) > id_getter_(txn);
        });
        return txns_.insert(pos, txn);
    }

    Iterator Erase(Iterator it) { return txns_.erase(it); }

    void PopFront() { txns_.pop_front(); }

    bool   Empty() const { return txns_.empty(); }
    size_t Size() const { return txns_.size(); }

private:
    TxnList txns_;

    absl::FunctionRef<TxnId(Transaction*)> id_getter_;
};

} // namespace idlekv
