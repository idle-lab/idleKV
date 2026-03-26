#pragma once


#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
namespace idlekv {


class Record {
public:

private:
    uint64_t lsn_;
    uint32_t crc_;
    std::string payload_;
};


class WAL {
public:
    enum Status {
        WaitingForRecovery,
        Running,
        Stoped,
    };


    WAL(std::string& dir) : dir_(dir) {
        if(std::filesystem::exists(dir)) {
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
