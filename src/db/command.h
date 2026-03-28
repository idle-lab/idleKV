#pragma once

#include "db/db.h"
#include "db/shard.h"
#include "redis/connection.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace idlekv {



class ExecContext {
public:
    ExecContext(Connection* conn, size_t OwnerId)
        : conn_(conn), owner_id_(OwnerId) {
        db_index_ = conn_->DbIndex();
    }

    auto GetConnection() -> Connection* { return conn_; }

    auto InitShard(Shard* shard) -> void { shard_ = shard; }

    auto GetDb() -> DB* { return shard_->DbAt(db_index_); }
    auto GetShard() -> Shard* { return shard_; }
    auto OwnerId() -> size_t { return owner_id_; }

private:
    Connection* conn_;

    Shard* shard_;
    size_t db_index_;
    size_t owner_id_;
};

// ExecFunc is interface for command executor
using Exector = auto (*)(ExecContext* ctx, std::vector<std::string>& args) -> void;

// PreFunc analyses command line when queued command to `multi`
// returns related write keys and read keys
using Prepare = auto (*)(const std::vector<std::string>& args)
    -> std::pair<std::vector<std::string>, std::vector<std::string>>;

enum class CmdFlags : uint32_t {
    None          = 0,
    CanExecInline = 1U << 0,
};

constexpr auto operator|(CmdFlags lhs, CmdFlags rhs) -> CmdFlags {
    using Underlying = std::underlying_type_t<CmdFlags>;
    return static_cast<CmdFlags>(static_cast<Underlying>(lhs) | static_cast<Underlying>(rhs));
}

constexpr auto operator&(CmdFlags lhs, CmdFlags rhs) -> CmdFlags {
    using Underlying = std::underlying_type_t<CmdFlags>;
    return static_cast<CmdFlags>(static_cast<Underlying>(lhs) & static_cast<Underlying>(rhs));
}

constexpr auto HasFlag(CmdFlags flags, CmdFlags flag) -> bool {
    using Underlying = std::underlying_type_t<CmdFlags>;
    return (static_cast<Underlying>(flags & flag)) != 0;
}

class Cmd {
public:
    Cmd(const std::string& name, int32_t arity, int32_t FirstKey, int32_t LastKey, Exector exector,
        Prepare prepare, CmdFlags flags = CmdFlags::None)
        : name_(name), arity_(arity), first_key_(FirstKey), last_key_(LastKey), exec_(exector),
          prepare_(prepare), flags_(flags) {}

    auto Exec(ExecContext* ctx, std::vector<std::string>& args) const -> void {
        return exec_(ctx, args);
    }

    auto PrepareKeys(const std::vector<std::string>& args) const
        -> std::pair<std::vector<std::string>, std::vector<std::string>> {
        return prepare_(args);
    }

    auto Verification(const std::vector<std::string>& args) const -> bool {
        if (arity_ == 0)
            return true;

        if (arity_ < 0) {
            return int32_t(args.size()) >= -arity_;
        }
        return int32_t(args.size()) == arity_;
    }

    auto Name() const -> std::string { return name_; }
    auto Arity() const -> int32_t { return arity_; }
    auto FirstKey() const -> int32_t { return first_key_; }
    auto LastKey() const -> int32_t { return last_key_; }
    auto Flags() const -> CmdFlags { return flags_; }
    auto HasFlag(CmdFlags flag) const -> bool { return idlekv::HasFlag(flags_, flag); }
    auto CanExecInline() const -> bool { return HasFlag(CmdFlags::CanExecInline); }

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
    Prepare  prepare_;
    CmdFlags flags_{CmdFlags::None};
};

} // namespace idlekv
