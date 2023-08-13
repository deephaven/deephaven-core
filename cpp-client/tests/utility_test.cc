/*
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
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
  const char *tz_key = "TZ";
  const char *original_tz = getenv(tz_key);
  setenv(tz_key, "America/Denver", 1);
  tzset();
  auto d1 = EpochMillisToStr(1689136824000);
  auto d2 = EpochMillisToStr(123456);
  CHECK(d1 == "2023-07-11T22:40:24.000-0600");
  CHECK(d2 == "1969-12-31T17:02:03.456-0700");
  if (original_tz != nullptr) {
    setenv(tz_key, original_tz, 1);
  } else {
    unsetenv(tz_key);
  }
  tzset();
}

TEST_CASE("ObjectId", "[utility]") {
  uintptr_t p = 0xdeadbeef;
  auto id = ObjectId("hello", (void *) p);
  CHECK(id == "hello[0xdeadbeef]");
}
}  // namespace deephaven::client::tests
