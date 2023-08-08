/*
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::utility::base64Encode;
using deephaven::dhcore::utility::epochMillisToStr;
using deephaven::dhcore::utility::objectId;

namespace deephaven::client::tests {
TEST_CASE("Base64encode", "[utility]") {
  // https://en.wikipedia.org/wiki/Base64
  CHECK(base64Encode("light work.") == "bGlnaHQgd29yay4=");
  CHECK(base64Encode("light work") == "bGlnaHQgd29yaw==");
  CHECK(base64Encode("light wor") == "bGlnaHQgd29y");
}

TEST_CASE("EpochMillisToStr", "[utility]") {
  const char *tzKey = "TZ";
  const char *originalTz = getenv(tzKey);
  setenv(tzKey, "America/Denver", 1);
  tzset();
  auto d1 = epochMillisToStr(1689136824000);
  auto d2 = epochMillisToStr(123456);
  CHECK(d1 == "2023-07-11T22:40:24.000-0600");
  CHECK(d2 == "1969-12-31T17:02:03.456-0700");
  if (originalTz != nullptr) {
    setenv(tzKey, originalTz, 1);
  } else {
    unsetenv(tzKey);
  }
  tzset();
}

TEST_CASE("ObjectId", "[utility]") {
  uintptr_t p = 0xdeadbeef;
  auto id = objectId("hello", (void*)p);
  CHECK(id == "hello[0xdeadbeef]");
}
}  // namespace deephaven::client::tests
