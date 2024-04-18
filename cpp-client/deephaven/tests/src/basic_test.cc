/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/client/client.h"

using deephaven::client::Client;

namespace deephaven::client::tests {
TEST_CASE("Close plays nice with destructor", "[simple]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();
  auto updated = table.Update("QQQ = i");
  std::cout << updated.Stream(true) << '\n';
  tm.Client().Close();
}
}  // namespace deephaven::client::tests
