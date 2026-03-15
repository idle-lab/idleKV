#include "db/command.h"

#include <gtest/gtest.h>

namespace {

using idlekv::Cmd;
using idlekv::CmdContext;
using idlekv::CmdFlags;
using idlekv::ExecResult;

auto noop_exec(CmdContext*, const std::vector<std::string>&) -> ExecResult {
    return ExecResult::ok();
}

auto noop_prepare(const std::vector<std::string>&)
    -> std::pair<std::vector<std::string>, std::vector<std::string>> {
    return {};
}

TEST(CmdTest, CanExecInlineIsExposedAsFlag) {
    Cmd cmd("ping", 1, -1, -1, noop_exec, noop_prepare, CmdFlags::CanExecInline);

    EXPECT_TRUE(cmd.can_exec_inline());
    EXPECT_TRUE(cmd.has_flag(CmdFlags::CanExecInline));
}

TEST(CmdTest, FlagHelpersRemainComposable) {
    const auto flags = CmdFlags::CanExecInline | CmdFlags::None;

    EXPECT_TRUE(idlekv::has_flag(flags, CmdFlags::CanExecInline));
    EXPECT_FALSE(idlekv::has_flag(flags, CmdFlags::None));
}

} // namespace
