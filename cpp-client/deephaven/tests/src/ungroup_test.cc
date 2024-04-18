/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"

namespace deephaven::client::tests {
// TODO(kosak): This test is currently disabled (by membership in the [.] test group, because we
//  don't yet deserialize the grouped column correctly.
TEST_CASE("Ungroup columns", "[.]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  table = table.Where("ImportDate == `2017-11-01`");

  auto by_table = table.Where("Ticker == `AAPL`").View("Ticker", "Close").View("Ticker");
  auto ungrouped = by_table.Ungroup("Close");

  std::vector<std::string> ticker_data = {"AAPL"};
  std::vector<std::string> close_data = {"[23.5,24.2,26.7]"};

  CompareTable(
      by_table,
      "Ticker", ticker_data,
      "Close", close_data
  );

  std::vector<std::string> ug_ticker_data = {"AAPL", "AAPL", "AAPL"};
  std::vector<double> ug_close_data = {23.5, 24.2, 26.7};

  CompareTable(
      ungrouped,
      "Ticker", ug_ticker_data,
      "Close", ug_close_data
  );
}
}  // namespace deephaven::client::tests
