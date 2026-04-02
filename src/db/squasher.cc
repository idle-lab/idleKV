#include "db/squasher.h"
#include "db/shard.h"

namespace idlekv {

auto CmdSquasher::Squash(std::vector<CommandContext>& cmds, Sender& sender) -> void {
    std::vector<ShardId> cmd_order;
    cmd_order.reserve(cmds.size());

    for (auto& cmd_ctx : cmds) {

    }
}


} // namespace idlekv