/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */
#include "tests/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;

namespace deephaven {
namespace client {
namespace tests {
// This example shows explicit QueryTable cleanup using destructors/RAII. Generally you would not
// need to worry about this, but you may want to destroy some tables more eagerly if you find you
// are using too much resources at the server.
TEST_CASE("Table Cleanup", "[tablecleanup]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  auto importDate = table.getStrCol("ImportDate");
  auto ticker = table.getStrCol("Ticker");
  // This example will dispose each table individually. This might be handy
  // but not necessary as you can depend on the context to clean up
  {
    auto t1 = table.where(importDate == "2017-11-01");
    {
      auto t2 = t1.countBy(ticker);
      std::cout << t2.stream(true) << '\n';
    }
    std::cerr << "t2 has been cleaned up.\n";
  }
  std::cerr << "t1 has been cleaned up.\n";
}
}  // namespace tests
}  // namespace client
}  // namespace deephaven
