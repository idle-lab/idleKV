#pragma once

#include "db/storage/art/art.h"

#include <memory_resource>
#include <string>
namespace idlekv {

class ZSet {
public:
    explicit ZSet(std::pmr::memory_resource* mr) : data_(mr) {}

private:
    Art<std::string> data_;
};

} // namespace idlekv