/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/client/client.h"

using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {
TEST_CASE("Filter a Table", "[filter]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  auto t1 = table.Where(
      "ImportDate == `2017-11-01` && Ticker == `AAPL` && (Close <= 120.0 || isNull(Close))");
  std::cout << t1.Stream(true) << '\n';

  TableMaker expected;
  expected.AddColumn<std::string>("ImportDate", {"2017-11-01", "2017-11-01", "2017-11-01"});
  expected.AddColumn<std::string>("Ticker", {"AAPL", "AAPL", "AAPL"});
  expected.AddColumn<double>("Open", {22.1, 26.8, 31.5});
  expected.AddColumn<double>("Close", {23.5, 24.2, 26.7});
  expected.AddColumn<int64_t>("Volume", {100000, 250000, 19000});

  TableComparerForTests::Compare(expected, t1);
}
}  // namespace deephaven::client::tests
