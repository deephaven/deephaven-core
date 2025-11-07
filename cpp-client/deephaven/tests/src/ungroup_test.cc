/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"

using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {
TEST_CASE("Ungroup columns", "[ungroup]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  table = table.Where("ImportDate == `2017-11-01`");

  auto by_table = table.Where("Ticker == `AAPL`").View("Ticker", "Close").By("Ticker");
  std::cout << by_table.Stream(true) << '\n';
  auto ungrouped = by_table.Ungroup("Close");
  std::cout << ungrouped.Stream(true) << '\n';

  {
    TableMaker expected;
    expected.AddColumn<std::string>("Ticker", {"AAPL"});
    expected.AddColumn<std::vector<double>>("Close", {{23.5, 24.2, 26.7}});
    TableComparerForTests::Compare(expected, by_table);
  }

  {
    TableMaker expected;
    expected.AddColumn<std::string>("Ticker", {"AAPL", "AAPL", "AAPL"});
    expected.AddColumn<double>("Close", {23.5, 24.2, 26.7});
    TableComparerForTests::Compare(expected, ungrouped);
  }
}
}  // namespace deephaven::client::tests
