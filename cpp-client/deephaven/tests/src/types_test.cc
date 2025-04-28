/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/types.h"
#include "deephaven/third_party/catch.hpp"
#include "deephaven/third_party/fmt/core.h"

using deephaven::dhcore::ElementType;
using deephaven::dhcore::ElementTypeId;

namespace deephaven::client::tests {
TEST_CASE("ElementTypeToString", "[types]") {
  auto int_type = ElementType::Of(ElementTypeId::kInt32);
  auto datetime_type = ElementType::Of(ElementTypeId::kTimestamp);

  CHECK("int32" == int_type.ToString());
  CHECK("list<int32>" == int_type.WrapList().ToString());
  CHECK("DateTime" == datetime_type.ToString());
  CHECK("list<DateTime>" == datetime_type.WrapList().ToString());
}

TEST_CASE("ElementTypeFmt", "[types]") {
  auto int_type = ElementType::Of(ElementTypeId::kInt32);
  CHECK("Hello int32" == fmt::format("Hello {}", int_type));
}

TEST_CASE("UnwrapThrows", "[types]") {
  auto int_type = ElementType::Of(ElementTypeId::kInt32);
  auto list_int_type = int_type.WrapList();

  CHECK_THROWS(int_type.UnwrapList());
  CHECK_NOTHROW(list_int_type.UnwrapList());
  CHECK_THROWS(list_int_type.UnwrapList().UnwrapList());
}
}  // namespace deephaven::client::tests
