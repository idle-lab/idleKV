#pragma once

#include <cstdint>
#include <optional>
#include <system_error>

namespace idlekv {

template <class T>
class ResultT {
public:
    ResultT(std::error_code ec, T res) : ec_(ec), res_(std::move(res)) {}

    ResultT(std::error_code ec) : ec_(ec) {}

    ResultT(T res) : res_(std::move(res)) {}

    auto ok() const -> bool { return !ec_; }

    auto value() -> T& { return *res_; }

    auto err() const -> const std::error_code& { return ec_; }

private:
    std::error_code  ec_ = std::error_code{};
    std::optional<T> res_;
};

} // namespace idlekv
