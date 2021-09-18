/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */
#include "tests/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::highlevel::BooleanExpression;
using deephaven::client::highlevel::NumericExpression;
using deephaven::client::highlevel::TableHandleManager;
using deephaven::client::highlevel::TableHandle;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;

namespace deephaven {
namespace client {
namespace tests {
TEST_CASE("Last By", "[lastby]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  auto importDate = table.getStrCol("ImportDate");
  auto ticker = table.getStrCol("Ticker");
  auto open = table.getNumCol("Open");
  auto close = table.getNumCol("Close");

  auto lb = table.where(importDate == "2017-11-01").select(ticker, open, close).lastBy(ticker);

  std::vector<std::string> tickerData = {"XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA"};
  std::vector<double> openData = {50.5, 92.3, 40.1, 681.43, 31.5, 685.3};
  std::vector<double> closeData = {53.8, 88.5, 38.7, 453, 26.7, 544.9};

  INFO(lb.stream(true));
  compareTable(
      lb,
      "Ticker", tickerData,
      "Open", openData,
      "Close", closeData
      );
}
}  // namespace tests
}  // namespace client
}  // namespace deephaven
