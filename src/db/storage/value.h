#pragma once
#include "common/config.h"
#include "common/logger.h"
#include "db/storage/zset.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <memory_resource>
#include <string_view>

namespace idlekv {

class Value;

class Str {
public:
    Str() = default;
    explicit Str(char* ptr, uint64_t size) : ptr_(ptr), size_(size) {}

    auto operator==(std::string_view s) -> bool {
        if (s.size() != size_) {
            return false;
        }
        return std::memcmp(ptr_, s.data(), s.size()) == 0;
    }

    auto Get() const -> std::string_view { return {ptr_, size_}; }

private:
    friend class Value;

    char*    ptr_{nullptr};
    uint32_t size_{0};
} __attribute__((packed));

class Value {
public:
    // 0-16 is reserved for inline lengths of string type.
    enum TypeEnum : uint8_t {
        STR  = 17,
        INT  = 18,
        ZSET = 19,
    };

    static auto InitMr(std::pmr::memory_resource* mr) -> void { value_mr = mr; }

    explicit Value() {}

    DISABLE_COPY_MOVE(Value);

    auto InitString(std::string_view sv) -> void {
        CHECK(len_tag_ == 0);
        if (sv.size() <= kInlineStrMaxLen) {
            len_tag_ = sv.size();
            std::memcpy(value_.inline_str, sv.data(), sv.size());
        } else {
            len_tag_  = STR;
            auto* ptr = value_mr->allocate(sv.size(), alignof(char));
            std::memcpy(ptr, sv.data(), sv.size());
            new (&value_.str) Str(static_cast<char*>(ptr), sv.size());
        }
    }

    auto InitZSet() -> void {
        void* ptr   = value_mr->allocate(sizeof(ZSet), alignof(ZSet));
        value_.zset = new (ptr) ZSet(value_mr);

        len_tag_ = ZSET;
    }

    inline auto IsInlineStr() const -> bool { return len_tag_ <= kInlineStrMaxLen; }
    inline auto IsStr() const -> bool { return len_tag_ <= STR; }

    auto GetString() const -> std::string_view {
        if (IsInlineStr()) {
            return {value_.inline_str, static_cast<size_t>(len_tag_)};
        }
        return value_.str.Get();
    }
    auto GetZSet() -> ZSet* { return value_.zset; }

    auto SetTTL() -> void { has_ttl_ = true; }
    auto HasTTL() const -> bool { return has_ttl_; }

    auto Type() const -> TypeEnum {
        return len_tag_ <= STR ? STR : static_cast<TypeEnum>(len_tag_);
    }

    ~Value() { ReleaseValue(); }

private:
    auto ReleaseValue() -> void {
        if (len_tag_ <= kInlineStrMaxLen) {
            return;
        }

        switch (len_tag_) {
        case STR:
            value_mr->deallocate(value_.str.ptr_, value_.str.size_, alignof(char));
            break;
        case ZSET:
            value_.zset->~ZSet();
            value_mr->deallocate(value_.zset, sizeof(ZSet), alignof(ZSet));
            break;
        case INT:
            break;
        default:
            UNREACHABLE();
        }
    }

    auto ResetEmpty() -> void {
        len_tag_ = 0;
        has_ttl_ = false;
        std::memset(value_.inline_str, 0, kInlineStrMaxLen);
    }

    static constexpr size_t kInlineStrMaxLen = 12;

    union ValueUnio {
        char    inline_str[kInlineStrMaxLen];
        Str     str;
        ZSet*   zset __attribute__((packed));
        int64_t ival __attribute__((packed)); // TODO(cyb): for cmd 'INCR'

        ValueUnio() : inline_str() {}
    } value_;

    // The lower four bits store the length of the inline string 
    // The highest bit indicates whether the type is a string
    uint8_t len_tag_ : 5 {0};
    uint8_t has_ttl_ : 1 {false};

    inline thread_local static std::pmr::memory_resource* value_mr{nullptr};
};

static_assert(sizeof(Value) == 13);

using PrimeValue = std::shared_ptr<Value>;

template <Value::TypeEnum Tag, class... Args>
inline auto MakeValue(Args&&... args) -> PrimeValue {
    auto pv = std::make_shared<Value>();

    if constexpr (Tag == Value::STR) {
        pv->InitString(std::forward<Args>(args)...);
    } else if constexpr (Tag == Value::ZSET) {
        pv->InitZSet(std::forward<Args>(args)...);
    } else if constexpr (Tag == Value::INT) {
        CHECK(false) << "INT_TAG not implemented";
    } else {
        UNREACHABLE();
    }

    return pv;
}

} // namespace idlekv
