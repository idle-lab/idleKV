
#include "utils/cpu/basic.h"

namespace idlekv {

namespace utils {

auto GetOnlineCpus() -> cpu_set_t {
    cpu_set_t online_cpus;
    CPU_ZERO(&online_cpus);
    sched_getaffinity(0, sizeof(online_cpus), &online_cpus);
    return online_cpus;
}

auto GetOnlineCpusNum() -> size_t {
    auto cpus = GetOnlineCpus();
    return CPU_COUNT(&cpus);
}

} // namespace utils

} // namespace idlekv