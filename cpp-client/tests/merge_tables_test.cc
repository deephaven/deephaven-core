/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"

namespace deephaven::client::tests {
TEST_CASE("Merge Tables", "[merge]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  auto importDate = table.getStrCol("ImportDate");
  auto ticker = table.getStrCol("Ticker");

  table = table.where(importDate == "2017-11-01");

  // Run a merge by fetching two tables and them merging them
  auto aaplTable = table.where(ticker == "AAPL").tail(10);
  auto zngaTable = table.where(ticker == "ZNGA").tail(10);

  auto merged = aaplTable.merge({zngaTable});
  std::cout << merged.stream(true) << '\n';

  std::vector<std::string> importDateData = {"2017-11-01", "2017-11-01", "2017-11-01",
      "2017-11-01", "2017-11-01"};
  std::vector<std::string> tickerData = {"AAPL", "AAPL", "AAPL", "ZNGA", "ZNGA"};
  std::vector<double> openData = {22.1, 26.8, 31.5, 541.2, 685.3};
  std::vector<double> closeData = {23.5, 24.2, 26.7, 538.2, 544.9};
  std::vector<int64_t> volData = {100000, 250000, 19000, 46123, 48300};

  compareTable(
      merged,
      "ImportDate", importDateData,
      "Ticker", tickerData,
      "Open", openData,
      "Close", closeData,
      "Volume", volData
      );
}
}  // namespace deephaven::client::tests
