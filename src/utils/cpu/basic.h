#pragma once

#include <cstddef>
#include <sched.h>
namespace idlekv {

namespace utils {

auto get_online_cpus() -> cpu_set_t;

auto get_online_cpus_num() -> size_t;

} // namespace utils

} // namespace idlekv
