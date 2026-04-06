#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>
namespace idlekv {

struct Record {
    uint64_t    lsn;
    uint32_t    crc;
    std::string payload;
};

class WALFile {



};

class WAL {
public:
    enum Status {
        WaitingForRecovery,
        Running,
        Stoped,
    };

    WAL(std::string& dir) : dir_(dir) {
        if (std::filesystem::exists(dir)) {
            s_ = WaitingForRecovery;
            return;
        }
        std::filesystem::create_directory(dir);

        
    }

    auto Write(Record r) -> void;

private:
    std::string dir_;

    Status s_;

    std::jthread writer_;
};

} // namespace idlekv
