#include "common/config.h"
#include "db/command.h"
#include "db/engine.h"
#include "redis/connection.h"

#include <gtest/gtest.h>
#include <mimalloc.h>

namespace {

using namespace idlekv;

class EngineCommandTest : public ::testing::Test {
protected:
    void SetUp() override {
        heap_ = mi_heap_new();
        ASSERT_NE(heap_, nullptr);
        engine_ = std::make_unique<IdleEngine>(cfg_, heap_);
        conn_   = std::make_unique<Connection>(nullptr, engine_.get());
    }

    void TearDown() override {
        conn_.reset();
        engine_.reset();
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
        return engine_->exec(conn_.get(), owned);
    }

    Config                           cfg_;
    mi_heap_t*                       heap_ = nullptr;
    std::unique_ptr<IdleEngine>      engine_;
    std::unique_ptr<Connection>      conn_;
};

TEST_F(EngineCommandTest, SetGetAndDelUseDashStore) {
    auto set_res = exec({"set", "name", "idlekv"});
    EXPECT_TRUE(set_res.ok());
    EXPECT_EQ(set_res.type(), ExecResult::Type::kSimpleString);
    EXPECT_EQ(set_res.message(), "OK");

    auto get_res = exec({"get", "name"});
    EXPECT_TRUE(get_res.ok());
    EXPECT_EQ(get_res.type(), ExecResult::Type::kBulkString);
    EXPECT_EQ(get_res.message(), "idlekv");

    auto overwrite_res = exec({"set", "name", "dash"});
    EXPECT_TRUE(overwrite_res.ok());
    EXPECT_EQ(overwrite_res.type(), ExecResult::Type::kSimpleString);

    auto get_overwritten = exec({"get", "name"});
    EXPECT_TRUE(get_overwritten.ok());
    EXPECT_EQ(get_overwritten.type(), ExecResult::Type::kBulkString);
    EXPECT_EQ(get_overwritten.message(), "dash");

    auto del_res = exec({"del", "name"});
    EXPECT_TRUE(del_res.ok());
    EXPECT_EQ(del_res.type(), ExecResult::Type::kInteger);
    EXPECT_EQ(del_res.integer(), 1);

    auto get_missing = exec({"get", "name"});
    EXPECT_TRUE(get_missing.ok());
    EXPECT_EQ(get_missing.type(), ExecResult::Type::kNullBulkString);

    auto del_missing = exec({"del", "name"});
    EXPECT_TRUE(del_missing.ok());
    EXPECT_EQ(del_missing.type(), ExecResult::Type::kInteger);
    EXPECT_EQ(del_missing.integer(), 0);
}

TEST_F(EngineCommandTest, SelectProvidesDatabaseIsolation) {
    ASSERT_TRUE(exec({"set", "shared", "db0"}).ok());

    auto select_db1 = exec({"select", "1"});
    EXPECT_TRUE(select_db1.ok());
    EXPECT_EQ(select_db1.type(), ExecResult::Type::kSimpleString);
    EXPECT_EQ(conn_->db_index(), 1U);

    auto missing_in_db1 = exec({"get", "shared"});
    EXPECT_TRUE(missing_in_db1.ok());
    EXPECT_EQ(missing_in_db1.type(), ExecResult::Type::kNullBulkString);

    ASSERT_TRUE(exec({"set", "shared", "db1"}).ok());

    auto back_to_db0 = exec({"select", "0"});
    EXPECT_TRUE(back_to_db0.ok());
    EXPECT_EQ(conn_->db_index(), 0U);

    auto db0_value = exec({"get", "shared"});
    EXPECT_TRUE(db0_value.ok());
    EXPECT_EQ(db0_value.type(), ExecResult::Type::kBulkString);
    EXPECT_EQ(db0_value.message(), "db0");

    ASSERT_TRUE(exec({"select", "1"}).ok());
    auto db1_value = exec({"get", "shared"});
    EXPECT_TRUE(db1_value.ok());
    EXPECT_EQ(db1_value.type(), ExecResult::Type::kBulkString);
    EXPECT_EQ(db1_value.message(), "db1");
}

TEST_F(EngineCommandTest, InvalidCommandMetadataReturnsErrors) {
    auto wrong_arity = exec({"get"});
    EXPECT_FALSE(wrong_arity.ok());
    EXPECT_EQ(wrong_arity.type(), ExecResult::Type::kError);

    auto bad_select = exec({"select", "999"});
    EXPECT_FALSE(bad_select.ok());
    EXPECT_EQ(bad_select.type(), ExecResult::Type::kError);

    auto unknown = exec({"unknown"});
    EXPECT_FALSE(unknown.ok());
    EXPECT_EQ(unknown.type(), ExecResult::Type::kError);
}

} // namespace
