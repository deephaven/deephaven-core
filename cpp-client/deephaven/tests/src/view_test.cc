/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::TableHandleManager;
using deephaven::client::TableHandle;

namespace deephaven::client::tests {
TEST_CASE("View", "[View]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  // literal strings
  auto t1 = table.LastBy("Ticker").View("Ticker", "Close", "Volume");

  std::vector<std::string> ticker_data = {"XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA", "T"};
  std::vector<double> close_data = {53.8, 88.5, 38.7, 453, 26.7, 544.9, 13.4};
  std::vector<int64_t> vol_data = {87000, 6060842, 138000, 138000000, 19000, 48300, 1500};

  CompareTable(
      t1,
      "Ticker", ticker_data,
      "Close", close_data,
      "Volume", vol_data
  );
}
}  // namespace deephaven::client::tests
