/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::utility::base64Encode;

namespace deephaven::client::tests {
TEST_CASE("base64encode", "[utility]") {
  // https://en.wikipedia.org/wiki/Base64
  CHECK(base64Encode("light work.") == "bGlnaHQgd29yay4=");
  CHECK(base64Encode("light work") == "bGlnaHQgd29yaw==");
  CHECK(base64Encode("light wor") == "bGlnaHQgd29y");
}
}  // namespace deephaven::client::tests


