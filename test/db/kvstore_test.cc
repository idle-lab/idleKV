#include "db/storage/kvstore.h"

#include <gtest/gtest.h>
#include <memory>
#include <memory_resource>

namespace {

using idlekv::DataEntity;
using idlekv::DummyImpl;
using idlekv::KvStore;

TEST(KvStoreTest, GetReturnsBorrowedReferenceInsteadOfCopy) {
    std::pmr::monotonic_buffer_resource mr;
    KvStore<DummyImpl<std::string, std::shared_ptr<DataEntity>>> store(&mr);

    auto value = std::make_shared<DataEntity>(DataEntity::from_string("value"));
    ASSERT_TRUE(store.set("key", value).ok());

    auto first = store.get("key");
    ASSERT_TRUE(first.ok());
    ASSERT_NE(first.get(), nullptr);

    auto second = store.get("key");
    ASSERT_TRUE(second.ok());
    ASSERT_NE(second.get(), nullptr);

    EXPECT_EQ(first.get()->as_string(), "value");
    EXPECT_EQ(second.get()->as_string(), "value");
    EXPECT_EQ(first.get().get(), second.get().get());
}

} // namespace
