/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/utility/utility.h"

namespace deephaven::client::tests {
// TODO(kosak): This test is currently disabled (by membership in the [.] test group, because we
//  don't yet deserialize the grouped column correctly.
TEST_CASE("Ungroup columns", "[.]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  auto importDate = table.getStrCol("ImportDate");
  auto ticker = table.getStrCol("Ticker");

  table = table.where(importDate == "2017-11-01");

  auto byTable = table.where(ticker == "AAPL").view("Ticker", "Close").by("Ticker");
  std::cout << byTable.stream(true) << '\n';

  auto ungrouped = byTable.ungroup("Close");
  std::cout << ungrouped.stream(true) << '\n';

  std::vector<std::string> tickerData = {"AAPL"};
  std::vector<std::string> closeData = {"[23.5,24.2,26.7]"};

  compareTable(
      byTable,
      "Ticker", tickerData,
      "Close", closeData
      );

  std::vector<std::string> ugTickerData = {"AAPL", "AAPL", "AAPL"};
  std::vector<double> ugCloseData = {23.5, 24.2, 26.7};

  compareTable(
      ungrouped,
      "Ticker", ugTickerData,
      "Close", ugCloseData
      );
}
}  // namespace deephaven::client::tests
