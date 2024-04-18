/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"

using deephaven::client::TableHandleManager;
using deephaven::client::TableHandle;

namespace deephaven::client::tests {
TEST_CASE("Last By", "[lastby]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  auto lb = table.Where("ImportDate == `2017-11-01`")
      .Select("Ticker", "Open", "Close")
      .LastBy("Ticker");

  std::vector<std::string> ticker_data = {"XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA"};
  std::vector<double> open_data = {50.5, 92.3, 40.1, 681.43, 31.5, 685.3};
  std::vector<double> close_data = {53.8, 88.5, 38.7, 453, 26.7, 544.9};

  INFO(lb.Stream(true));
  CompareTable(
      lb,
      "Ticker", ticker_data,
      "Open", open_data,
      "Close", close_data
  );
}
}  // namespace deephaven::client::tests
