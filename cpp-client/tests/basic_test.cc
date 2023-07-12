/*
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/client.h"

using deephaven::client::Client;

namespace deephaven::client::tests {
TEST_CASE("Close plays nice with destructor", "[simple]") {
    auto tm = TableMakerForTests::create();
    auto table = tm.table();
    auto updated = table.update("QQQ = i");
    std::cout << updated.stream(true) << '\n';
    tm.client().close();
  }
}
