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

inline auto MultiReadKeys(const CmdArgs& args) -> WRSet {
    WRSet keys;
    if (args.size() < 2) {
        return keys;
    }

    keys.read_keys.reserve(args.size() - 1);
    for (size_t i = 1; i < args.size(); ++i) {
        keys.read_keys.push_back(i);
    }
    return keys;
}

inline auto MultiWriteKeys(const CmdArgs& args) -> WRSet {
    WRSet keys;
    if (args.size() < 2) {
        return keys;
    }

    keys.write_keys.reserve(args.size() - 1);
    for (size_t i = 1; i < args.size(); ++i) {
        keys.write_keys.push_back(i);
    }
    return keys;
}

} // namespace idlekv
