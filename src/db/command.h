#pragma once

#include "absl/container/inlined_vector.h"
#include "utils/range/concat_view.h"
#include "utils/time/time.h"

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>

namespace idlekv {

// using CmdArgs = std::vector<std::string>;

// Same as BackedArguments of dragonflydb.
class CmdArgs {
    constexpr static size_t kLenCap     = 5;
    constexpr static size_t kStorageCap = 88;

public:
    using value_type = std::string_view;

    CmdArgs() = default;
    class iterator {
    public:
        using iterator_category = std::random_access_iterator_tag;
        using value_type        = std::string_view;
        using difference_type   = std::ptrdiff_t;
        using pointer           = const std::string_view*;
        using reference         = std::string_view;

        iterator(const CmdArgs* ba, size_t index) : ba_(ba), index_(index) {}

        iterator& operator++() {
            ++index_;
            return *this;
        }

        iterator& operator--() {
            --index_;
            return *this;
        }

        iterator& operator+=(int delta) {
            index_ += delta;
            return *this;
        }

        iterator operator+(int delta) const {
            iterator res(*this);
            res += delta;
            return res;
        }

        ptrdiff_t operator-(iterator other) const {
            return ptrdiff_t(index_) - ptrdiff_t(other.index_);
        }

        bool operator==(const iterator& other) const {
            return index_ == other.index_ && ba_ == other.ba_;
        }

        bool operator!=(const iterator& other) const { return !(*this == other); }

        std::string_view operator*() const { return ba_->at(index_); }

    private:
        const CmdArgs* ba_;
        size_t         index_;
    };

    template <typename I>
    CmdArgs(I begin, I end, size_t len) {
        Assign(begin, end, len);
    }

    template <typename I>
    void Assign(I begin, I end, size_t len);

    void Reserve(size_t arg_cnt, size_t total_size) {
        offsets_.reserve(arg_cnt);
        storage_.reserve(total_size);
    }

    size_t HeapMemory() const {
        size_t s1 = offsets_.capacity() <= kLenCap ? 0 : offsets_.capacity() * sizeof(uint32_t);
        size_t s2 = storage_.capacity() <= kStorageCap ? 0 : storage_.capacity();
        return s1 + s2;
    }

    void SwapArgs(CmdArgs& other) {
        offsets_.swap(other.offsets_);
        storage_.swap(other.storage_);
    }

    // The capacity is chosen so that we allocate a fully utilized (128 bytes) block.
    using StorageType = absl::InlinedVector<char, kStorageCap>;

    std::string_view Front() const { return std::string_view{storage_.data(), elem_len(0)}; }

    size_t size() const { return offsets_.size(); }

    bool empty() const { return offsets_.empty(); }

    size_t elem_len(size_t i) const { return elem_capacity(i) - 1; }

    size_t elem_capacity(size_t i) const {
        uint32_t next_offs = i + 1 >= offsets_.size() ? storage_.size() : offsets_[i + 1];
        return next_offs - offsets_[i];
    }

    std::string_view at(uint32_t index) const {
        uint32_t offset = offsets_[index];
        return std::string_view{storage_.data() + offset, elem_len(index)};
    }

    char* data(uint32_t index) {
        uint32_t offset = offsets_[index];
        return storage_.data() + offset;
    }

    std::string_view operator[](uint32_t index) const { return at(index); }

    iterator begin() const { return {this, 0}; }

    iterator end() const { return {this, offsets_.size()}; }

    void clear() {
        // Clear the contents without deallocating memory. clear() deallocates inlined_vector.
        offsets_.resize(0);
        storage_.resize(0);
    }

    std::string_view back() const {
        assert(size() > 0);
        return at(size() - 1);
    }

    // Reserves space for additional argument of given length at the end.
    void PushArg(size_t len) {
        size_t old_size = storage_.size();
        offsets_.push_back(old_size);
        storage_.resize(old_size + len + 1);
    }

    void PushArg(std::string_view arg) {
        PushArg(arg.size());
        char* dest = storage_.data() + offsets_.back();
        if (arg.size() > 0)
            memcpy(dest, arg.data(), arg.size());
        dest[arg.size()] = '\0';
    }

    void PopArg() {
        uint32_t last_offs = offsets_.back();
        offsets_.pop_back();
        storage_.resize(last_offs);
    }

    void ClearForReuse() {
        offsets_.clear();
        if (HeapMemory() > 1024) {
            storage_.clear();
            offsets_.shrink_to_fit();
        }
    }

private:
    absl::InlinedVector<uint32_t, 5> offsets_;
    StorageType    storage_;
};

using CmdArgsPtr = std::unique_ptr<CmdArgs>;
struct PendingRequest {
    CmdArgs* args;
    TimePoint  started_at;
};

template <typename I>
void CmdArgs::Assign(I begin, I end, size_t len) {
    offsets_.resize(len);
    size_t   total_size = 0;
    unsigned idx        = 0;
    for (auto it = begin; it != end; ++it) {
        offsets_[idx++] = total_size;
        total_size += (*it).size() + 1; // +1 for '\0'
    }
    storage_.resize(total_size);

    // Reclaim memory if we have too much allocated.
    if (storage_.capacity() > kStorageCap && total_size < storage_.capacity() / 2)
        storage_.shrink_to_fit();

    char* next = storage_.data();
    for (auto it = begin; it != end; ++it) {
        size_t sz = (*it).size();
        if (sz > 0) {
            memcpy(next, (*it).data(), sz);
        }
        next[sz] = '\0';
        next += sz + 1;
    }
}

class ExecContext;

// ExecFunc is interface for command executor
using Exector = auto (*)(ExecContext* ctx, CmdArgs& args) -> void;

// key index in CmdArgs.
// For single key command, we use stack allocation to avoid unnecessary heap allocation. 
using KeySet = absl::InlinedVector<size_t, 1>;

struct WRSet {
    KeySet read_keys;
    KeySet write_keys;

    auto AllKeys() const {
        return utils::MakeConcatView(read_keys, write_keys);
    }
};

// PreFunc analyses command line when queued command to `multi`
// returns related write keys and read keys.
// All string_views are referenced from CmdArgs
using Prepare = auto (*)(const CmdArgs& args) -> WRSet;

enum class CmdFlags : uint32_t {
    None           = 0,
    CanExecInPlace = 1U << 0,
    NoKey          = 1U << 1,
    Transactional = 1U << 2, // command should be executed in transaction, e.g. multi/exec block
    StateChange  = 1U << 3, // command may be change connection state.
};

constexpr auto operator|(CmdFlags lhs, CmdFlags rhs) -> CmdFlags {
    using Underlying = std::underlying_type_t<CmdFlags>;
    return static_cast<CmdFlags>(static_cast<Underlying>(lhs) | static_cast<Underlying>(rhs));
}

constexpr auto operator&(CmdFlags lhs, CmdFlags rhs) -> CmdFlags {
    using Underlying = std::underlying_type_t<CmdFlags>;
    return static_cast<CmdFlags>(static_cast<Underlying>(lhs) & static_cast<Underlying>(rhs));
}

constexpr auto HasFlag(CmdFlags flags, CmdFlags flag) -> bool {
    using Underlying = std::underlying_type_t<CmdFlags>;
    return (static_cast<Underlying>(flags & flag)) != 0;
}

class Cmd {
public:
    Cmd(const std::string& name, int32_t arity, int32_t FirstKey, int32_t LastKey, Exector exector,
        Prepare prepare, CmdFlags flags = CmdFlags::None)
        : name_(name), arity_(arity), first_key_(FirstKey), last_key_(LastKey), exec_(exector),
          prepare_(prepare), flags_(flags) {}

    auto Exec(ExecContext* ctx, CmdArgs& args) const -> void { return exec_(ctx, args); }

    auto PrepareKeys(CmdArgs& args) const -> WRSet { return prepare_(args); }

    auto Verification(CmdArgs& args) const -> bool {
        if (arity_ == 0)
            return true;

        if (arity_ < 0) {
            return int32_t(args.size()) >= -arity_;
        }
        return int32_t(args.size()) == arity_;
    }

    auto Name() const -> std::string { return name_; }
    auto Arity() const -> int32_t { return arity_; }
    auto FirstKey() const -> int32_t { return first_key_; }
    auto LastKey() const -> int32_t { return last_key_; }
    auto Flags() const -> CmdFlags { return flags_; }
    auto HasFlag(CmdFlags flag) const -> bool { return idlekv::HasFlag(flags_, flag); }
    auto CanExecInPlace() const -> bool { return HasFlag(CmdFlags::CanExecInPlace); }
    auto IsTransactional() const -> bool { return HasFlag(CmdFlags::Transactional); }
    auto IsStateChange() const -> bool { return HasFlag(CmdFlags::StateChange); }

private:
    // name in lowercase letters
    std::string name_;

    // arity means allowed number of cmdArgs.
    // 1) arity < 0 means len(args) >= -arity.
    // 2) arity > 0 means len(args) == arity.
    // for example: the arity of `get` is 2, `mget` is -2
    int32_t arity_;

    int32_t first_key_;

    int32_t last_key_;

    Exector exec_;
    // prepare returns related keys command
    Prepare  prepare_;
    CmdFlags flags_{CmdFlags::None};
};

// Contains the context information required for a single instruction, including the instruction
// itself, parameters, and read/write key values.
struct CommandContext {
    Cmd*       cmd;
    CmdArgs*    args;
    WRSet      keys;

    TimePoint start_at;
};

} // namespace idlekv
