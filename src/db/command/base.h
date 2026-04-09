#pragma once

#include "db/command.h"

namespace idlekv {

inline auto SingleReadKey(const CmdArgs& args) -> WRSet {
    if (args.size() < 2) {
        return {};
    }
    return {{1}, {}};
}

inline auto SingleWriteKey(const CmdArgs& args) -> WRSet {
    if (args.size() < 2) {
        return {};
    }
    return {{}, {1}};
}

} // namespace idlekv