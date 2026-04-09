#include "utils/fiber/runtime.h"

#include <boost/system/detail/error_code.hpp>
#include <stdexcept>
#include <utility>

namespace boost::fibers::asio {} // namespace boost::fibers::asio

namespace idlekv {

boost::asio::io_context::id Priority::service::id;

} // namespace idlekv
