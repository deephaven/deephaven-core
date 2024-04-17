/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/client/client.h"

using deephaven::client::TableHandle;

namespace deephaven::client::tests {
TEST_CASE("Filter a Table", "[filter]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  auto t1 = table.Where(
      "ImportDate == `2017-11-01` && Ticker == `AAPL` && (Close <= 120.0 || isNull(Close))");
  std::cout << t1.Stream(true) << '\n';

  std::vector<std::string> import_date_data = {"2017-11-01", "2017-11-01", "2017-11-01"};
  std::vector<std::string> ticker_data = {"AAPL", "AAPL", "AAPL"};
  std::vector<double> open_data = {22.1, 26.8, 31.5};
  std::vector<double> close_data = {23.5, 24.2, 26.7};
  std::vector<int64_t> vol_data = {100000, 250000, 19000};

  CompareTable(
      t1,
      "ImportDate", import_date_data,
      "Ticker", ticker_data,
      "Open", open_data,
      "Close", close_data,
      "Volume", vol_data
  );
}
}  // namespace deephaven::client::tests
