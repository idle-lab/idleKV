#include "utils/range/concat_view.h"

#include <array>
#include <gtest/gtest.h>
#include <list>
#include <ranges>
#include <sstream>
#include <utility>
#include <vector>

namespace idlekv {
namespace {

template <std::ranges::input_range R>
auto Collect(R&& range) {
    using value_type = std::ranges::range_value_t<R>;

    std::vector<value_type> values;
    for (auto&& value : range) {
        values.emplace_back(value);
    }
    return values;
}

TEST(ConcatViewTest, ConcatenatesRangesWithSameIteratorType) {
    std::vector<int> first{1, 2};
    std::vector<int> second{3, 4};

    auto view = utils::MakeConcatView(first, second);

    static_assert(std::ranges::view<decltype(view)>);
    static_assert(std::ranges::forward_range<decltype(view)>);

    EXPECT_EQ(Collect(view), (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(view.size(), 4U);
}

TEST(ConcatViewTest, PreservesReferenceSemanticsForMutableLvalueRanges) {
    std::vector<int> first{1, 2};
    std::list<int>   second{3, 4};

    auto view = utils::MakeConcatView(first, second);

    for (int& value : view) {
        value += 10;
    }

    EXPECT_EQ(first, (std::vector<int>{11, 12}));
    EXPECT_EQ(std::vector<int>(second.begin(), second.end()), (std::vector<int>{13, 14}));
}

TEST(ConcatViewTest, HandlesEmptySides) {
    std::vector<int>   empty;
    std::array<int, 3> values{1, 2, 3};

    auto left_empty  = utils::MakeConcatView(empty, values);
    auto right_empty = utils::MakeConcatView(values, empty);

    EXPECT_EQ(Collect(left_empty), (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(Collect(right_empty), (std::vector<int>{1, 2, 3}));
}

TEST(ConcatViewTest, SupportsConstIteration) {
    std::vector<int> first{1, 2};
    std::list<int>   second{3, 4};

    const auto view = utils::MakeConcatView(first, second);

    static_assert(std::ranges::input_range<const decltype(view)>);

    EXPECT_EQ(Collect(view), (std::vector<int>{1, 2, 3, 4}));
}

TEST(ConcatViewTest, SupportsNonCommonSecondRange) {
    std::istringstream input("0 2 4 6");
    auto               even_numbers = std::ranges::istream_view<int>(input);

    static_assert(!std::ranges::common_range<decltype(even_numbers)>);

    auto view = utils::MakeConcatView(std::views::single(-1), std::move(even_numbers));

    EXPECT_EQ(Collect(view), (std::vector<int>{-1, 0, 2, 4, 6}));
}

TEST(ConcatViewTest, SupportsTemporaryViews) {
    EXPECT_EQ(Collect(utils::MakeConcatView(std::views::single(7), std::views::iota(8, 11))),
              (std::vector<int>{7, 8, 9, 10}));
}

} // namespace
} // namespace idlekv
