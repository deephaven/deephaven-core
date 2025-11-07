/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"

using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {
TEST_CASE("Merge Tables", "[Merge]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  table = table.Where("ImportDate == `2017-11-01`");

  // Run a merge by fetching two tables and them merging them
  auto aapl_table = table.Where("Ticker == `AAPL`").Tail(10);
  auto znga_table = table.Where("Ticker == `ZNGA`").Tail(10);

  auto merged = aapl_table.Merge({znga_table});
  std::cout << merged.Stream(true) << '\n';

  TableMaker expected;
  expected.AddColumn<std::string>("ImportDate", {"2017-11-01", "2017-11-01", "2017-11-01",
      "2017-11-01", "2017-11-01"});
  expected.AddColumn<std::string>("Ticker", {"AAPL", "AAPL", "AAPL", "ZNGA", "ZNGA"});
  expected.AddColumn<double>("Open", {22.1, 26.8, 31.5, 541.2, 685.3});
  expected.AddColumn<double>("Close", {23.5, 24.2, 26.7, 538.2, 544.9});
  expected.AddColumn<int64_t>("Volume", {100000, 250000, 19000, 46123, 48300});

  TableComparerForTests::Compare(expected, merged);
}
}  // namespace deephaven::client::tests
