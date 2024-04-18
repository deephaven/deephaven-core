/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"

using deephaven::client::Client;

namespace deephaven::client::tests {
TEST_CASE("On Close Callbacks can be added and removed and are executed", "[simple]") {
    auto client = TableMakerForTests::CreateClient();
    bool cb_1_called = false;
    auto cb_1 = [&cb_1_called]{ cb_1_called = true; };
    bool cb_2_called = false;
    auto cb_2 = [&cb_2_called]{ cb_2_called = true; };
    const auto id_1 = client.AddOnCloseCallback(cb_1);
    (void) client.AddOnCloseCallback(cb_2);
    const bool removed = client.RemoveOnCloseCallback(id_1);
    CHECK(removed);
    client.Close();
    CHECK(!cb_1_called);
    CHECK(cb_2_called);
  }
}
