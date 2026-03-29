#pragma once
#include <cstdint>
#include <string>
#include <variant>
namespace idlekv {

// TODO(cyb): Object impl to support hash, set...
class SmallString {

    static constexpr size_t kMaxSmallStringLen = 8;

    char data[kMaxSmallStringLen];
};

class String {

};

class Object {
public:
    enum class Type : uint8_t {
        kUnknow,
        kString,
        kHash,
        kSet,
    };

    Object() = default;

    union {
        SmallString ss_;
        String s_;


    } obj_;

};

} // namespace idlekv
