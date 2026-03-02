#pragma once


#include "db/db.h"
#include "redis/connection.h"
#include <memory>
namespace idlekv {

// all contextual information required for command execution
class Context {
public:
    explicit Context(const std::shared_ptr<Connection>& conn) : owner_(conn) {}

    Context(const Context&) = delete;
    auto operator=(const Context&) -> Context& = delete;

    auto connection() const -> Connection* { return owner_.get(); }

    auto db() const -> DB* {return cur_db_.get(); }

    auto select_db(const std::shared_ptr<DB>& db_ref) -> void { cur_db_ = db_ref; }

private:
    std::shared_ptr<DB> cur_db_;
    std::shared_ptr<Connection> owner_;

};

} // namespace idlekv