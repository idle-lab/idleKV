#pragma once
#include "common/config.h"
#include "common/logger.h"
#include "db/storage/zset.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory_resource>
#include <string_view>

namespace idlekv {

class PrimeValue;

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

    auto Get() -> std::string_view { return {ptr_, size_}; }
private:
    friend class PrimeValue;

    char* ptr_{nullptr};
    uint64_t size_{0};
} __attribute__((packed));


enum class ObjectType : uint8_t {
    Unknow,
    ZSet,
};

class Object {
public:
    Object(ObjectType type, void* ptr) : ptr_(ptr), type_(type) { }

    DISABLE_MOVE(Object);

    auto Type() -> ObjectType { return type_; }

    template<class T>
    auto GetAs() -> T* { return static_cast<T*>(ptr_); }

private:
    friend class PrimeValue;

    void* ptr_;
    ObjectType type_{ObjectType::Unknow};
} __attribute__((packed));


class PrimeValue {
    static constexpr size_t kInlineStrMaxLen = 16;
    union ValueUnio {
        char inline_str[kInlineStrMaxLen];
        Str str;
        Object obj;

        ValueUnio() : str() {}
    };

    class ValueRef {
    public:
        explicit ValueRef(PrimeValue& val) : value_ref_(std::ref(val)) {
            val.ref_count_.fetch_add(1, std::memory_order_relaxed);
        }

        DISABLE_COPY(ValueRef);

        auto Value() -> PrimeValue& { return value_ref_.get(); }

        ~ValueRef() {
            value_ref_.get().ref_count_.fetch_sub(1, std::memory_order_relaxed);
        }
    private:
        std::reference_wrapper<PrimeValue> value_ref_;
    };

    // 0-16 is reserved for inline lengths of string type.
    enum TagEnum : uint8_t {
        STR_TAG = 17,
        OBJ_TAG = 18,
    };

public:
    static auto InitMr(std::pmr::memory_resource* mr) -> void { value_mr = mr; }

    explicit PrimeValue() = default;

    DISABLE_COPY_MOVE(PrimeValue);

    auto SetString(std::string_view sv) -> void {
        CHECK(len_tag_ == 0);
        if (sv.size() <= kInlineStrMaxLen) {
            len_tag_ = sv.size();
            std::memcpy(value_.inline_str, sv.data(), sv.size());
        } else {
            len_tag_ = STR_TAG;
            auto* ptr = value_mr->allocate(sv.size(), alignof(char));
            std::memcpy(ptr, sv.data(), sv.size());
            new (&value_.str) Str(static_cast<char*>(ptr), sv.size());
        }
    }

    auto InitZSet() -> void {
        void *ptr = value_mr->allocate(sizeof(ZSet), alignof(ZSet));
        new (ptr) ZSet();
        new (&value_.obj) Object(ObjectType::ZSet, ptr); 

        len_tag_ = OBJ_TAG;
    }

    auto IsInlineStr() -> bool {
        return len_tag_ <= kInlineStrMaxLen;
    }

    auto GetString() -> std::string_view {
        if (IsInlineStr()) {
            return {value_.inline_str, static_cast<size_t>(len_tag_)};
        }
        return value_.str.Get();
    }

    auto SetTTL() -> void { has_ttl_ = true; }
    auto SetDeleting() -> void { deleting_ = true; }
    auto HasTTL() -> bool { return has_ttl_; }
    auto IsDeleting() -> bool { return deleting_; }

    auto GetObject() -> Object& {
        return value_.obj;
    }

    auto Borrow() -> ValueRef {
        return ValueRef(*this);
    }

    auto RefCount() -> uint16_t { return ref_count_.load(std::memory_order_relaxed); }

    ~PrimeValue() {
        CHECK_EQ(ref_count_.load(std::memory_order_relaxed), 0);
        CHECK(deleting_);

        if (len_tag_ <= 16) {
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
            default:
                UNREACHABLE();
            }
            break;
        default:
            UNREACHABLE();
        }
    }

private:
    ValueUnio value_;
    // To facilitate zero-copy transmission reply, we should keep track of reference counts to guard against dangling pointers.
    std::atomic_uint16_t ref_count_{0};
    // 低四位存内联字符串的长度
    // 最高位存类型是否为字符串
    uint8_t len_tag_ : 5 {0};
    uint8_t has_ttl_ : 1 {false};
    uint8_t deleting_ : 1 {false};

    inline thread_local static std::pmr::memory_resource* value_mr{nullptr};
};

static_assert(sizeof(PrimeValue) == 20);

} // namespace idlekv
