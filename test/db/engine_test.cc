#include "common/config.h"
#include "db/db.h"
#include "db/engine.h"
#include "db/result.h"
#include "db/xmalloc.h"
#include "redis/connection.h"
#include "redis/error.h"
#include "server/el_pool.h"
#include "server/thread_state.h"

#include <asio/awaitable.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <mimalloc.h>
#include <string>
#include <system_error>
#include <vector>

namespace {

using namespace idlekv;

class LoopbackConnection final : public Connection {
public:
    LoopbackConnection() : Connection(nullptr) {}

    auto read_impl(byte*, size_t) noexcept -> asio::awaitable<ResultT<size_t>> override {
        co_return ResultT<size_t>(asio::error::operation_not_supported);
    }

    auto write_impl(const byte* data, size_t size) noexcept
        -> asio::awaitable<ResultT<size_t>> override {
        output_.append(data, size);
        co_return ResultT<size_t>(std::error_code{}, size);
    }

    auto writev_impl(const std::vector<BufView>& bufs) noexcept
        -> asio::awaitable<ResultT<size_t>> override {
        size_t written = 0;
        for (const auto& buf : bufs) {
            output_.append(buf.data(), buf.size());
            written += buf.size();
        }
        co_return ResultT<size_t>(std::error_code{}, written);
    }

    auto output() const -> const std::string& { return output_; }

private:
    std::string output_;
};

class EngineCommandTest : public ::testing::Test {
protected:
    void SetUp() override {
        heap_ = mi_heap_new();
        ASSERT_NE(heap_, nullptr);

        allocator_ = std::make_unique<XAllocator>(heap_);

        engine  = std::make_unique<IdleEngine>(cfg_);
        engine_ = engine.get();
        init_systemcmd(engine_);
        init_strings(engine_);

        conn_ = std::make_unique<Connection>(nullptr);

        dbs_.reserve(cfg_.db_num_);
        for (size_t i = 0; i < cfg_.db_num_; ++i) {
            dbs_.emplace_back(std::make_unique<DB>(allocator_.get()));
        }
    }

    void TearDown() override {
        dbs_.clear();
        conn_.reset();

        engine_ = nullptr;
        engine.reset();

        allocator_.reset();
        if (heap_ != nullptr) {
            mi_heap_delete(heap_);
            heap_ = nullptr;
        }
    }

    auto exec(std::initializer_list<const char*> args) -> ExecResult {
        std::vector<std::string> owned;
        owned.reserve(args.size());
        for (const auto* arg : args) {
            owned.emplace_back(arg);
        }

        auto* cmd = engine_->get_cmd(owned[0]);
        if (cmd == nullptr) {
            return ExecResult::error(fmt::format(kUnknownCmdErrFmt, owned[0]));
        }

        if (!cmd->verification(owned)) {
            return ExecResult::error(fmt::format(kArgNumErrFmt, cmd->name()));
        }

        CmdContext ctx(conn_.get(), dbs_[conn_->db_index()].get());
        return cmd->exec(&ctx, owned);
    }

    Config                           cfg_;
    mi_heap_t*                       heap_ = nullptr;
    std::unique_ptr<XAllocator>      allocator_;
    IdleEngine*                      engine_ = nullptr;
    std::unique_ptr<Connection>      conn_;
    std::vector<std::unique_ptr<DB>> dbs_;
};

TEST_F(EngineCommandTest, SetGetAndDelUseDashStore) {
    auto set_res = exec({"set", "name", "idlekv"});
    EXPECT_TRUE(set_res.is_ok());
    EXPECT_EQ(set_res.type(), ExecResult::kOk);

    auto get_res = exec({"get", "name"});
    EXPECT_EQ(get_res.type(), ExecResult::kBulkString);
    EXPECT_EQ(get_res.string(), "idlekv");

    auto overwrite_res = exec({"set", "name", "dash"});
    EXPECT_TRUE(overwrite_res.is_ok());
    EXPECT_EQ(overwrite_res.type(), ExecResult::kOk);

    auto get_overwritten = exec({"get", "name"});
    EXPECT_EQ(get_overwritten.type(), ExecResult::kBulkString);
    EXPECT_EQ(get_overwritten.string(), "dash");

    auto del_res = exec({"del", "name"});
    EXPECT_EQ(del_res.type(), ExecResult::kInteger);
    EXPECT_EQ(del_res.integer(), 1);

    auto get_missing = exec({"get", "name"});
    EXPECT_EQ(get_missing.type(), ExecResult::kNull);

    auto del_missing = exec({"del", "name"});
    EXPECT_EQ(del_missing.type(), ExecResult::kInteger);
    EXPECT_EQ(del_missing.integer(), 0);
}

TEST_F(EngineCommandTest, SelectProvidesDatabaseIsolation) {
    ASSERT_TRUE(exec({"set", "shared", "db0"}).is_ok());

    auto select_db1 = exec({"select", "1"});
    EXPECT_TRUE(select_db1.is_ok());
    EXPECT_EQ(select_db1.type(), ExecResult::kOk);
    EXPECT_EQ(conn_->db_index(), 1U);

    auto missing_in_db1 = exec({"get", "shared"});
    EXPECT_EQ(missing_in_db1.type(), ExecResult::kNull);

    ASSERT_TRUE(exec({"set", "shared", "db1"}).is_ok());

    auto back_to_db0 = exec({"select", "0"});
    EXPECT_TRUE(back_to_db0.is_ok());
    EXPECT_EQ(conn_->db_index(), 0U);

    auto db0_value = exec({"get", "shared"});
    EXPECT_EQ(db0_value.type(), ExecResult::kBulkString);
    EXPECT_EQ(db0_value.string(), "db0");

    ASSERT_TRUE(exec({"select", "1"}).is_ok());
    auto db1_value = exec({"get", "shared"});
    EXPECT_EQ(db1_value.type(), ExecResult::kBulkString);
    EXPECT_EQ(db1_value.string(), "db1");
}

TEST_F(EngineCommandTest, InvalidCommandMetadataReturnsErrors) {
    auto wrong_arity = exec({"get"});
    EXPECT_EQ(wrong_arity.type(), ExecResult::kError);
    EXPECT_EQ(wrong_arity.string(), "ERR wrong number of arguments for 'get' command");

    auto bad_select = exec({"select", "999"});
    EXPECT_EQ(bad_select.type(), ExecResult::kError);
    EXPECT_EQ(bad_select.string(), "ERR DB index is out of range");

    auto unknown = exec({"unknown"});
    EXPECT_EQ(unknown.type(), ExecResult::kError);
    EXPECT_EQ(unknown.string(), "Err unknown command 'unknown'");
}

TEST(EngineAsyncExecTest, CrossShardExecRoundTripsReplies) {
    Config        cfg;
    EventLoopPool pool(2);
    pool.run();
    pool.await_foreach([](size_t i, EventLoop* el) { ThreadState::init(i, el, el->thread_id()); });

    engine = std::make_unique<IdleEngine>(cfg);
    engine->init(&pool);

    auto* idle_engine = engine.get();
    ASSERT_NE(idle_engine, nullptr);

    std::string cross_shard_key;
    for (int i = 0; i < 4096; ++i) {
        auto candidate = "cross-shard-" + std::to_string(i);
        if (idle_engine->calculate_shard_id(candidate) != 0U) {
            cross_shard_key = std::move(candidate);
            break;
        }
    }
    ASSERT_FALSE(cross_shard_key.empty());

    LoopbackConnection conn;

    pool.at(0)->await_dispatch([&]() -> asio::awaitable<void> {
        std::vector<std::string> set_args = {"set", cross_shard_key, "value"};
        co_await idle_engine->exec(&conn, set_args);
        co_await conn.sender().flush();

        std::vector<std::string> get_args = {"get", cross_shard_key};
        co_await idle_engine->exec(&conn, get_args);
        co_await conn.sender().flush();
    }());

    EXPECT_EQ(conn.output(), "+OK\r\n$5\r\nvalue\r\n");

    engine.reset();
    pool.stop();
}

} // namespace
