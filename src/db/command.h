#pragma once

#include "db/db.h"
#include "db/result.h"
#include "redis/connection.h"

#include <asio/awaitable.hpp>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace idlekv {


class CmdContext {
public:
    CmdContext(Connection* conn, DB* db) : conn_(conn), db_(db) {}

    auto connection() -> Connection* { return conn_; }

    auto db() -> DB* { return db_; }

private:
    Connection* conn_;

    DB* db_ = nullptr;
};

// ExecFunc is interface for command executor
using Exector = auto (*)(CmdContext* ctx, const std::vector<std::string>& args) -> ExecResult;

// PreFunc analyses command line when queued command to `multi`
// returns related write keys and read keys
using Prepare = auto (*)(const std::vector<std::string>& args)
    -> std::pair<std::vector<std::string>, std::vector<std::string>>;

class Cmd {
public:
    Cmd(const std::string& name, int32_t arity, int32_t first_key, int32_t last_key,
        Exector exector, Prepare prepare)
        : name_(name), arity_(arity), first_key_(first_key), last_key_(last_key), exec_(exector),
          prepare_(prepare) {}

    auto exec(CmdContext* ctx, const std::vector<std::string>& args) const -> ExecResult {
        return exec_(ctx, args);
    }

    auto prepare(const std::vector<std::string>& args) const
        -> std::pair<std::vector<std::string>, std::vector<std::string>> {
        return prepare_(args);
    }

    auto verification(const std::vector<std::string>& args) const -> bool {
        if (arity_ == 0)
            return true;

        if (arity_ < 0) {
            return int32_t(args.size()) >= -arity_;
        }
        return int32_t(args.size()) == arity_;
    }

    auto name() const -> std::string { return name_; }
    auto arity() const -> int32_t { return arity_; }
    auto first_key() const -> int32_t { return first_key_; }
    auto last_key() const -> int32_t { return last_key_; }

private:
    // name in lowercase letters
    std::string name_;

    // arity means allowed number of cmdArgs.
    // 1) arity < 0 means len(args) >= -arity.
    // 2) arity > 0 means len(args) == arity.
    // for example: the arity of `get` is 2, `mget` is -2
    int32_t arity_;

    int32_t first_key_;

    int32_t last_key_;

    Exector exec_;
    // prepare returns related keys command
    Prepare prepare_;
};



} // namespace idlekv
