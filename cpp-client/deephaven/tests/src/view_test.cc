/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {
TEST_CASE("View", "[View]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  // literal strings
  auto t1 = table.LastBy("Ticker").View("Ticker", "Close", "Volume");

  TableMaker expected;
  expected.AddColumn<std::string>("Ticker", {"XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA", "T"});
  expected.AddColumn<double>("Close", {53.8, 88.5, 38.7, 453, 26.7, 544.9, 13.4});
  expected.AddColumn<int64_t>("Volume", {87000, 6060842, 138000, 138000000, 19000, 48300, 1500});
  TableComparerForTests::Compare(expected, t1);
}
}  // namespace deephaven::client::tests
