#pragma once

#include <cstddef>
#include <sched.h>
namespace idlekv {

namespace utils {

auto GetOnlineCpus() -> cpu_set_t;

auto GetOnlineCpusNum() -> size_t;

} // namespace utils

} // namespace idlekv
