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
    uint64_t size_{0};
} __attribute__((packed));

enum class ObjectType : uint8_t {
    Unknow,
    ZSet,
    Set,
    Hash,
};

// Redis container object, such as Hash, Sorted Set, Set etc.
class Object {
public:
    Object(ObjectType type, void* ptr) : ptr_(ptr), type_(type) {}

    DISABLE_MOVE(Object);

    auto Type() const -> ObjectType { return type_; }

    template <class T>
    auto GetAs() const -> T* {
        return static_cast<T*>(ptr_);
    }

private:
    friend class Value;

    void*      ptr_;
    ObjectType type_{ObjectType::Unknow};
} __attribute__((packed));

class Value {
public:
    // 0-16 is reserved for inline lengths of string type.
    enum TagEnum : uint8_t {
        STR_TAG = 17,
        OBJ_TAG = 18,
        INT_TAG = 19,
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
            len_tag_  = STR_TAG;
            auto* ptr = value_mr->allocate(sv.size(), alignof(char));
            std::memcpy(ptr, sv.data(), sv.size());
            new (&value_.str) Str(static_cast<char*>(ptr), sv.size());
        }
    }

    auto InitZSet() -> void {
        void* ptr = value_mr->allocate(sizeof(ZSet), alignof(ZSet));
        new (ptr) ZSet(value_mr);
        new (&value_.obj) Object(ObjectType::ZSet, ptr);

        len_tag_ = OBJ_TAG;
    }

    inline auto IsInlineStr() const -> bool { return len_tag_ <= kInlineStrMaxLen; }
    inline auto IsStr() const -> bool { return len_tag_ <= STR_TAG; }

    auto GetString() const -> std::string_view {
        if (IsInlineStr()) {
            return {value_.inline_str, static_cast<size_t>(len_tag_)};
        }
        return value_.str.Get();
    }

    auto SetTTL() -> void { has_ttl_ = true; }
    auto HasTTL() const -> bool { return has_ttl_; }

    auto GetObject() const -> const Object& { return value_.obj; }

    ~Value() { ReleaseValue(); }

private:
    auto ReleaseValue() -> void {
        if (len_tag_ <= kInlineStrMaxLen) {
            return;
        }

        switch (len_tag_) {
        case STR_TAG:
            value_mr->deallocate(value_.str.ptr_, value_.str.size_, alignof(char));
            break;
        case OBJ_TAG:
            switch (value_.obj.Type()) {
            case ObjectType::ZSet:
                value_.obj.GetAs<ZSet>()->~ZSet();
                value_.obj.~Object();
                value_mr->deallocate(value_.obj.ptr_, sizeof(ZSet), alignof(ZSet));
                break;
            default:
                UNREACHABLE();
            }
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

    static constexpr size_t kInlineStrMaxLen = 16;

    union ValueUnio {
        char    inline_str[kInlineStrMaxLen];
        Str     str;
        Object  obj;
        int64_t ival __attribute__((packed));

        ValueUnio() : inline_str() {}
    } value_;

    // 低四位存内联字符串的长度
    // 最高位存类型是否为字符串
    uint8_t len_tag_ : 5 {0};
    uint8_t has_ttl_ : 1 {false};

    inline thread_local static std::pmr::memory_resource* value_mr{nullptr};
};

static_assert(sizeof(Value) == 17);

using PrimeValue = std::shared_ptr<Value>;

struct MakeValueOption {
    Value::TagEnum tag;
    ObjectType     type{ObjectType::Unknow};
};

template <class... Args>
inline auto MakeObjectValue(Value& value, ObjectType type, Args&&... args) -> void {
    switch (type) {
    case ObjectType::ZSet:
        value.InitZSet(std::forward<Args>(args)...);
        return;
    case ObjectType::Set:
    case ObjectType::Hash:
        CHECK(false) << "object type not implemented";
        return;
    default:
        UNREACHABLE();
    }
}

template <Value::TagEnum Tag, class... Args>
inline auto MakeValue(Args&&... args) -> PrimeValue {
    auto pv = std::make_shared<Value>();

    if constexpr (Tag == Value::STR_TAG) {
        pv->InitString(std::forward<Args>(args)...);
    } else if constexpr (Tag == Value::OBJ_TAG) {
        MakeObjectValue(*pv, std::forward<Args>(args)...);
    } else if constexpr (Tag == Value::INT_TAG) {
        CHECK(false) << "INT_TAG not implemented";
    } else {
        UNREACHABLE();
    }

    return pv;
}

} // namespace idlekv
