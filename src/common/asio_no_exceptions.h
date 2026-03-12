#pragma once

#if defined(ASIO_NO_EXCEPTIONS)

#include <asio/detail/throw_exception.hpp>
#include <concepts>
#include <cstdio>
#include <cstdlib>
#include <typeinfo>

namespace asio {
namespace detail {

template <typename Exception>
[[noreturn]] void throw_exception(const Exception& e ASIO_SOURCE_LOCATION_PARAM) {
    if constexpr (requires {
                      { e.what() } -> std::convertible_to<const char*>;
                  }) {
        std::fprintf(stderr, "asio fatal: %s\n", e.what());
    } else {
        std::fprintf(stderr, "asio fatal: %s\n", typeid(Exception).name());
    }

    std::fflush(stderr);
    std::abort();
}

} // namespace detail
} // namespace asio

#endif // defined(ASIO_NO_EXCEPTIONS)
