
#include "db/command.h"
#include "db/engine.h"

namespace idlekv {

auto set(Connection* ctx, const std::vector<std::string>& args) -> ExecResult {
    // Implementation for set command
    return ExecResult(ExecResult::Status::OK, "OK");
}

auto init_strings(IdleEngine* eng) -> void { eng->register_cmd("set", -3, 1, 1, set, nullptr); }

} // namespace idlekv
