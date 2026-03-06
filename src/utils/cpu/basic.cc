
#include "utils/cpu/basic.h"

namespace idlekv {

namespace utils {

auto get_online_cpus() -> cpu_set_t {
    cpu_set_t online_cpus;
    CPU_ZERO(&online_cpus);
    sched_getaffinity(0, sizeof(online_cpus), &online_cpus);
    return online_cpus;
}

auto get_online_cpus_num() -> size_t {
    auto cpus = get_online_cpus();
    return CPU_COUNT(&cpus);
}

} // namespace utils

} // namespace idlekv