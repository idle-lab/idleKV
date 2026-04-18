#pragma once

#include <cstdint>
namespace idlekv {

using TxnId                  = uint64_t;
constexpr TxnId InvalidTxnId = 0;
using KeyFingerprint         = uint64_t;

} // namespace idlekv
