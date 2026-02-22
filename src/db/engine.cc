#include "db/engine.h"


namespace idlekv {

auto IdleEngine::init_command() -> void {

}

auto IdleEngine::select_db(size_t idx) -> std::shared_ptr<DB> {
    if (idx < 0 || idx >= db_set_.size()) {
        return nullptr;
    }
    return db_set_[idx];
}



} // namespace idlekv
