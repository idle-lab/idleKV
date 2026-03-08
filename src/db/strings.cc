
#include "db/command.h"
#include "db/engine.h"

namespace idlekv {

auto set(Connection* ctx, const std::vector<std::string>& args) -> std::string {}

auto init_strings(IdleEngine* eng) -> void { eng->register_cmd("set", -3, 1, 1, set, nullptr); }

} // namespace idlekv
