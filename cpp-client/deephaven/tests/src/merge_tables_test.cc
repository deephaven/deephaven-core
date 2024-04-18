/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"

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

  std::vector<std::string> import_date_data = {"2017-11-01", "2017-11-01", "2017-11-01",
      "2017-11-01", "2017-11-01"};
  std::vector<std::string> ticker_data = {"AAPL", "AAPL", "AAPL", "ZNGA", "ZNGA"};
  std::vector<double> open_data = {22.1, 26.8, 31.5, 541.2, 685.3};
  std::vector<double> close_data = {23.5, 24.2, 26.7, 538.2, 544.9};
  std::vector<int64_t> vol_data = {100000, 250000, 19000, 46123, 48300};

  CompareTable(
      merged,
      "ImportDate", import_date_data,
      "Ticker", ticker_data,
      "Open", open_data,
      "Close", close_data,
      "Volume", vol_data
  );
}
}  // namespace deephaven::client::tests
