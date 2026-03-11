#pragma once

#include "common/logger.h"
#include <cstddef>
#include <string>
#include <vector>

namespace idlekv {

class Args {
public:

    auto push_back(std::string arg) -> void {
        offsets_.push_back(data_.size());
        data_ += std::move(arg);
    }

    auto get(size_t index) const -> std::string_view {
        CHECK(index < offsets_.size()) << "index out of bounds";
        size_t start = offsets_[index];
        size_t end = (index + 1 < offsets_.size()) ? offsets_[index + 1] : data_.size();
        return std::string_view(data_.data() + start, end - start);
    }

    auto size() const -> size_t { return offsets_.size(); }

    auto shrink_to_fit() -> void {
        data_.shrink_to_fit();
        offsets_.shrink_to_fit();
    }
private:

    std::string data_;
    std::vector<size_t> offsets_;
};


} // namespace idlekv