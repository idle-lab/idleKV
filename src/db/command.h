#pragma once

#include "db/db.h"
#include "redis/connection.h"
#include "redis/protocol/reply.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace idlekv {

// ExecFunc is interface for command executor
using Exector = auto (*)(std::shared_ptr<DB> db, std::shared_ptr<Connection> conn, const std::vector<std::string>& args)
    -> std::unique_ptr<Reply>;

// PreFunc analyses command line when queued command to `multi`
// returns related write keys and read keys
using Prepare = auto (*)(const std::vector<std::string>& args)
    -> std::pair<std::vector<std::string>, std::vector<std::string>>;

class Cmd {
public:
    Cmd(const std::string& name, int32_t arity, bool is_systemcmd, Exector exector, Prepare prepare)
        : name_(name), arity_(arity), is_systemcmd_(is_systemcmd), exec_(exector), prepare_(prepare) {}

    auto exec(std::shared_ptr<DB> db, std::shared_ptr<Connection> conn, const std::vector<std::string>& args) const
        -> std::unique_ptr<Reply> {
        return exec_(db, conn, args);
    }

    auto prepare(const std::vector<std::string>& args) const
        -> std::pair<std::vector<std::string>, std::vector<std::string>> {
        return prepare_(args);
    }

    auto verification(const std::vector<std::string>& args) const -> bool {
        if (arity_ < 0) {
            return int32_t(args.size()) >= -arity_;
        }
        return int32_t(args.size()) == arity_;
    }

    auto is_systemcmd() const -> bool { return is_systemcmd_; }

    auto name() const -> std::string { return name_; }

    auto arity() const -> int32_t { return arity_; }

private:
    // name in lowercase letters
    std::string name_;

    // arity means allowed number of cmdArgs, arity < 0 means len(args) >= -arity.
    // for example: the arity of `get` is 2, `mget` is -2
    int32_t arity_;

    bool is_systemcmd_;

    Exector exec_;
    // prepare returns related keys command
    Prepare prepare_;
};

} // namespace idlekv
