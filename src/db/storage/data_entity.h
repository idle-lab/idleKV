#pragma once
#include <cstdint>
#include <string>
namespace idlekv {

class DataEntity {
public:
    enum class Type : uint8_t {
        kUnknow,
        kString,
        kHash,
        kSet,
    };

    DataEntity() = default;

    static auto FromString(std::string value) -> DataEntity {
        return DataEntity(Type::kString, std::move(value));
    }

    auto GetType() const -> Type { return type_; }

    auto IsString() const -> bool { return type_ == Type::kString; }

    auto AsString() const -> const std::string& { return string_value_; }

    auto operator==(const DataEntity&) const -> bool = default;

private:
    DataEntity(Type type, std::string value) : type_(type), string_value_(std::move(value)) {}

    Type        type_ = Type::kString;
    std::string string_value_;
};

} // namespace idlekv
