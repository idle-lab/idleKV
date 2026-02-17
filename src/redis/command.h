#pragma once

#include <string>
#include <vector>

namespace idlekv {

class Cmd {
public:
    explicit Cmd(std::vector<std::string> args) : args_(std::move(args)) {}

private:
    std::vector<std::string> args_;
};

} // namespace idlekv
