#include <chrono>
#include <iostream>
#include <thread>
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/client.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::Client;
using deephaven::client::NumCol;
using deephaven::client::StrCol;
using deephaven::client::TableHandle;
using deephaven::client::utility::TableMaker;
using deephaven::dhcore::utility::streamf;
using deephaven::dhcore::utility::stringf;

namespace deephaven::client::tests {

TEST_CASE("Clean shutdown", "[cleanup]") {
  {
    auto tm = TableMakerForTests::create();
    auto table = tm.table();
    auto updated = table.update("QQQ = i");
    std::cout << updated.stream(true) << '\n';

    // also do some subscribes
  }
}
}  // namespace deephaven::client::tests
