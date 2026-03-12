#include "db/engine.h"
#include "redis/connection.h"

#include <string>

namespace idlekv {

auto select(Connection* conn, const std::vector<std::string>& args) -> std::string {}

auto init_systemcmd(IdleEngine* eng) -> void {}

} // namespace idlekv
