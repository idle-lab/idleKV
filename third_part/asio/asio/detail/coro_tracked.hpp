#ifndef ASIO_DETAIL_CORO_TRACKED_HPP
#define ASIO_DETAIL_CORO_TRACKED_HPP

#include "asio/detail/config.hpp"

#include <cstdint>

#if defined(IDLEKV_ENABLE_CORO_TRACKING)
# include "server/coro_tracking.h"
#endif // defined(IDLEKV_ENABLE_CORO_TRACKING)

namespace asio {
namespace detail {

template <typename> class awaitable_thread;

class CoroTracked
{
public:
  template <typename Executor>
  static void on_resume(awaitable_thread<Executor>* thread) noexcept
  {
#if defined(IDLEKV_ENABLE_CORO_TRACKING)
    if (!thread || !idlekv::has_thread_state())
      return;

    if (!thread->has_tracked_coro())
    {
      auto coro_id = idlekv::coro_tracking_on_startup();
      if (coro_id != 0)
        thread->set_tracked_coro_id(coro_id);
      return;
    }

    idlekv::coro_tracking_on_resume(thread->tracked_coro_id());
#else // defined(IDLEKV_ENABLE_CORO_TRACKING)
    (void)thread;
#endif // defined(IDLEKV_ENABLE_CORO_TRACKING)
  }

  template <typename Executor>
  static void on_suspend(awaitable_thread<Executor>* thread) noexcept
  {
#if defined(IDLEKV_ENABLE_CORO_TRACKING)
    if (!thread || !thread->has_tracked_coro() || !idlekv::has_thread_state())
      return;

    idlekv::coro_tracking_on_suspend_or_finish(thread->tracked_coro_id(), false);
#else // defined(IDLEKV_ENABLE_CORO_TRACKING)
    (void)thread;
#endif // defined(IDLEKV_ENABLE_CORO_TRACKING)
  }

  template <typename Executor>
  static void on_finish(awaitable_thread<Executor>* thread) noexcept
  {
#if defined(IDLEKV_ENABLE_CORO_TRACKING)
    if (!thread)
      return;

    if (thread->has_tracked_coro() && idlekv::has_thread_state())
      idlekv::coro_tracking_on_suspend_or_finish(thread->tracked_coro_id(), true);

    thread->clear_tracked_coro();
#else // defined(IDLEKV_ENABLE_CORO_TRACKING)
    (void)thread;
#endif // defined(IDLEKV_ENABLE_CORO_TRACKING)
  }
};

} // namespace detail
} // namespace asio

#endif // ASIO_DETAIL_CORO_TRACKED_HPP
