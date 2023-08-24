/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"

using deephaven::client::BooleanExpression;
using deephaven::client::NumericExpression;
using deephaven::client::TableHandleManager;
using deephaven::client::TableHandle;
using deephaven::dhcore::utility::Streamf;
using deephaven::dhcore::utility::Stringf;

namespace deephaven::client::tests {
TEST_CASE("Last By", "[lastby]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  auto import_date = table.GetStrCol("ImportDate");
  auto ticker = table.GetStrCol("Ticker");
  auto open = table.GetNumCol("Open");
  auto close = table.GetNumCol("Close");

  auto lb = table.Where(import_date == "2017-11-01").Select(ticker, open, close).LastBy(ticker);

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
