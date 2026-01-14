#pragma once

#include <condition_variable>
#include <fstream>
#include <mutex>
#include <thread>
#include <vector>

namespace idlekv {

// WAL记录类型
enum class WALRecordType : uint8_t { PUT = 1, DELETE = 2, COMMIT = 3, ABORT = 4, CHECKPOINT = 5 };

// WAL记录结构
struct WALRecord {};

// WAL管理器
class WALManager {};

} // namespace idlekv