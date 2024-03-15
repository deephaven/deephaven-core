/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::utility::Base64Encode;
using deephaven::dhcore::utility::EpochMillisToStr;
using deephaven::dhcore::utility::ObjectId;

namespace deephaven::client::tests {
TEST_CASE("Base64encode", "[utility]") {
  // https://en.wikipedia.org/wiki/Base64
  CHECK(Base64Encode("light work.") == "bGlnaHQgd29yay4=");
  CHECK(Base64Encode("light work") == "bGlnaHQgd29yaw==");
  CHECK(Base64Encode("light wor") == "bGlnaHQgd29y");
}

TEST_CASE("EpochMillisToStr", "[utility]") {
  auto d1 = EpochMillisToStr(1689136824000);
  auto d2 = EpochMillisToStr(123456);
  CHECK(d1 == "2023-07-12T04:40:24.000Z");
  CHECK(d2 == "1970-01-01T00:02:03.456Z");
}

TEST_CASE("ObjectId", "[utility]") {
  uintptr_t p = 0xdeadbeef;
  auto id = ObjectId("hello", reinterpret_cast<void *>(p));
  CHECK(id == "hello(0xdeadbeef)");
}
}  // namespace deephaven::client::tests
