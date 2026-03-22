#pragma once

#include "db/storage/dash/bucket.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace idlekv::dash::detail {

template <class Key, class Value, size_t RegularBuckets, size_t StashBuckets, size_t SlotsPerBucket>
class Segment {
    static_assert(RegularBuckets >= 2, "Dash segment needs at least two regular buckets");
    static_assert(StashBuckets >= 1, "Dash segment needs stash buckets");
    static_assert(SlotsPerBucket >= 1, "Dash bucket must have at least one slot");

public:
    using RecordType = Record<Key, Value>;
    using RecordPtr  = std::shared_ptr<RecordType>;
    using BucketType = Bucket<RecordType, SlotsPerBucket>;

    static constexpr size_t kRegularBucketCount = RegularBuckets;
    static constexpr size_t kStashBucketCount   = StashBuckets;
    static constexpr size_t kBucketCount        = RegularBuckets + StashBuckets;
    static constexpr size_t kSlotsPerBucket     = SlotsPerBucket;


};

} // namespace idlekv::dash::detail
