#include "db/storage/kvstore.h"

#include <gtest/gtest.h>
#include <memory_resource>

namespace {

using idlekv::DataEntity;
using idlekv::DummyImpl;
using idlekv::KvStore;

TEST(KvStoreTest, GetReturnsBorrowedReferenceInsteadOfCopy) {
    std::pmr::monotonic_buffer_resource mr;
    KvStore<DummyImpl<std::string, DataEntity>> store(&mr);

    ASSERT_TRUE(store.set("key", DataEntity::from_string("value")).ok());

    auto first = store.get("key");
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(first.get().has_value());

    auto second = store.get("key");
    ASSERT_TRUE(second.ok());
    ASSERT_TRUE(second.get().has_value());

    EXPECT_EQ(first.get()->as_string_view(), "value");
    EXPECT_EQ(second.get()->as_string_view(), "value");
    EXPECT_EQ(&first.get().get(), &second.get().get());
}

} // namespace
