#pragma once

#include <concepts>
#include <cstddef>
#include <iterator>
#include <optional>
#include <ranges>
#include <type_traits>
#include <utility>
#include <variant>

namespace idlekv::utils {

namespace detail {

template <bool Const, class T>
using maybe_const_t = std::conditional_t<Const, const T, T>;

template <class R1, class R2>
concept concat_reference_compatible = requires {
    typename std::common_type_t<std::ranges::range_value_t<R1>, std::ranges::range_value_t<R2>>;
    typename std::common_reference_t<std::ranges::range_reference_t<R1>,
                                     std::ranges::range_reference_t<R2>>;
    typename std::common_reference_t<std::ranges::range_rvalue_reference_t<R1>,
                                     std::ranges::range_rvalue_reference_t<R2>>;
};

template <class R1, class R2>
concept concat_range_constraints = std::ranges::input_range<R1> && std::ranges::input_range<R2> &&
                                   concat_reference_compatible<R1, R2>;

} // namespace detail

template <std::ranges::view V1, std::ranges::view V2>
    requires detail::concat_range_constraints<V1, V2>
class ConcatView : public std::ranges::view_interface<ConcatView<V1, V2>> {
public:
    ConcatView()
        requires std::default_initializable<V1> && std::default_initializable<V2>
    = default;

    constexpr ConcatView(V1 v1, V2 v2) : v1_(std::move(v1)), v2_(std::move(v2)) {}

    template <bool Const>
    class iterator;

    template <bool Const>
    class sentinel;

    constexpr auto begin() { return MakeBegin<false>(*this); }

    constexpr auto begin() const
        requires detail::concat_range_constraints<const V1, const V2>
    {
        return MakeBegin<true>(*this);
    }

    constexpr auto end() { return sentinel<false>{std::ranges::end(v2_)}; }

    constexpr auto end() const
        requires detail::concat_range_constraints<const V1, const V2>
    {
        return sentinel<true>{std::ranges::end(v2_)};
    }

    constexpr auto size()
        requires std::ranges::sized_range<V1> && std::ranges::sized_range<V2>
    {
        using size_type =
            std::common_type_t<std::ranges::range_size_t<V1>, std::ranges::range_size_t<V2>>;
        return static_cast<size_type>(std::ranges::size(v1_)) +
               static_cast<size_type>(std::ranges::size(v2_));
    }

    constexpr auto size() const
        requires std::ranges::sized_range<const V1> && std::ranges::sized_range<const V2>
    {
        using size_type = std::common_type_t<std::ranges::range_size_t<const V1>,
                                             std::ranges::range_size_t<const V2>>;
        return static_cast<size_type>(std::ranges::size(v1_)) +
               static_cast<size_type>(std::ranges::size(v2_));
    }

private:
    template <bool Const, class Self>
    static constexpr auto MakeBegin(Self& self) {
        using Base1 = detail::maybe_const_t<Const, V1>;
        using Base2 = detail::maybe_const_t<Const, V2>;
        using current_t =
            std::variant<std::ranges::iterator_t<Base1>, std::ranges::iterator_t<Base2>>;

        auto begin1 = std::ranges::begin(self.v1_);
        auto end1   = std::ranges::end(self.v1_);
        auto begin2 = std::ranges::begin(self.v2_);

        if (begin1 == end1) {
            return iterator<Const>{current_t{std::in_place_index<1>, std::move(begin2)},
                                   std::move(end1), std::nullopt};
        }

        return iterator<Const>{current_t{std::in_place_index<0>, std::move(begin1)},
                               std::move(end1),
                               std::optional<std::ranges::iterator_t<Base2>>{std::move(begin2)}};
    }

    V1 v1_{};
    V2 v2_{};
};

template <std::ranges::view V1, std::ranges::view V2>
    requires detail::concat_range_constraints<V1, V2>
template <bool Const>
class ConcatView<V1, V2>::iterator {
private:
    using Base1 = detail::maybe_const_t<Const, V1>;
    using Base2 = detail::maybe_const_t<Const, V2>;

    using current_t = std::variant<std::ranges::iterator_t<Base1>, std::ranges::iterator_t<Base2>>;

    friend class ConcatView<V1, V2>;
    friend class sentinel<Const>;

    constexpr iterator(current_t current, std::ranges::sentinel_t<Base1> end1,
                       std::optional<std::ranges::iterator_t<Base2>> second_begin)
        : current_(std::move(current)), end1_(std::move(end1)),
          second_begin_(std::move(second_begin)) {}

public:
    using iterator_concept =
        std::conditional_t<std::ranges::forward_range<Base1> && std::ranges::forward_range<Base2>,
                           std::forward_iterator_tag, std::input_iterator_tag>;
    using iterator_category = iterator_concept;
    using value_type =
        std::common_type_t<std::ranges::range_value_t<Base1>, std::ranges::range_value_t<Base2>>;
    using difference_type =
        std::common_type_t<std::ranges::range_difference_t<Base1>,
                           std::ranges::range_difference_t<Base2>, std::ptrdiff_t>;
    using reference        = std::common_reference_t<std::ranges::range_reference_t<Base1>,
                                              std::ranges::range_reference_t<Base2>>;
    using rvalue_reference = std::common_reference_t<std::ranges::range_rvalue_reference_t<Base1>,
                                                     std::ranges::range_rvalue_reference_t<Base2>>;

    iterator() = default;

    constexpr auto operator*() const -> reference {
        if (current_.index() == 0) {
            return *std::get<0>(current_);
        }
        return *std::get<1>(current_);
    }

    constexpr auto operator++() -> iterator& {
        if (current_.index() == 0) {
            auto& it = std::get<0>(current_);
            ++it;
            if (it == end1_) {
                current_.template emplace<1>(std::move(*second_begin_));
                second_begin_.reset();
            }
        } else {
            ++std::get<1>(current_);
        }
        return *this;
    }

    constexpr auto operator++(int) -> iterator
        requires(std::ranges::forward_range<Base1> && std::ranges::forward_range<Base2>)
    {
        auto tmp = *this;
        ++(*this);
        return tmp;
    }

    constexpr void operator++(int)
        requires(!(std::ranges::forward_range<Base1> && std::ranges::forward_range<Base2>))
    {
        ++(*this);
    }

    friend constexpr auto operator==(const iterator& lhs, const iterator& rhs) -> bool
        requires std::equality_comparable<std::ranges::iterator_t<Base1>> &&
                 std::equality_comparable<std::ranges::iterator_t<Base2>>
    {
        return lhs.current_ == rhs.current_;
    }

    friend constexpr auto iter_move(const iterator& it) -> rvalue_reference {
        if (it.current_.index() == 0) {
            return std::ranges::iter_move(std::get<0>(it.current_));
        }
        return std::ranges::iter_move(std::get<1>(it.current_));
    }

private:
    current_t                                     current_{};
    std::ranges::sentinel_t<Base1>                end1_{};
    std::optional<std::ranges::iterator_t<Base2>> second_begin_{};
};

template <std::ranges::view V1, std::ranges::view V2>
    requires detail::concat_range_constraints<V1, V2>
template <bool Const>
class ConcatView<V1, V2>::sentinel {
private:
    using Base2 = detail::maybe_const_t<Const, V2>;

    friend class ConcatView<V1, V2>;

    constexpr explicit sentinel(std::ranges::sentinel_t<Base2> end2) : end2_(std::move(end2)) {}

public:
    sentinel() = default;

    friend constexpr auto operator==(const iterator<Const>& it, const sentinel& s) -> bool {
        return it.current_.index() == 1 && std::get<1>(it.current_) == s.end2_;
    }

    friend constexpr auto operator==(const sentinel& s, const iterator<Const>& it) -> bool {
        return it == s;
    }

private:
    std::ranges::sentinel_t<Base2> end2_{};
};

template <std::ranges::viewable_range R1, std::ranges::viewable_range R2>
    requires detail::concat_range_constraints<std::views::all_t<R1>, std::views::all_t<R2>>
constexpr auto MakeConcatView(R1&& r1, R2&& r2) {
    return ConcatView<std::views::all_t<R1>, std::views::all_t<R2>>{
        std::views::all(std::forward<R1>(r1)),
        std::views::all(std::forward<R2>(r2)),
    };
}

} // namespace idlekv::utils

namespace std::ranges {

template <class V1, class V2>
inline constexpr bool enable_borrowed_range<idlekv::utils::ConcatView<V1, V2>> =
    borrowed_range<V1> && borrowed_range<V2>;

} // namespace std::ranges
