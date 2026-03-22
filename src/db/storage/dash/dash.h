#pragma once

#include "db/storage/dash/segment.h"

#include <cstddef>
#include <functional>

#include <type_traits>
#


namespace idlekv::dash {

template <class Key, class Value, class Hash = std::hash<Key>, class KeyEqual = std::equal_to<Key>,
          size_t RegularBuckets = 56, size_t StashBuckets = 4, size_t SlotsPerBucket = 14>
class DashEH {
    static_assert(std::is_copy_constructible_v<Key>,
                  "DashEH currently requires copyable key types");
    static_assert(std::is_copy_constructible_v<Value>,
                  "DashEH currently requires copyable value types");

public:
    using SegmentType = detail::Segment<Key, Value, RegularBuckets, StashBuckets, SlotsPerBucket>;
    using RecordType  = typename SegmentType::RecordType;

};

} // namespace idlekv::dash
