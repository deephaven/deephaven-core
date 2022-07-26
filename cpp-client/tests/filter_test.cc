/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/client.h"

using deephaven::client::TableHandle;

namespace deephaven::client::tests {
TEST_CASE("Filter a table", "[filter]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  auto importDate = table.getStrCol("ImportDate");
  auto ticker = table.getStrCol("Ticker");
  auto close = table.getNumCol("Close");

  auto t1 = table.where(
      "ImportDate == `2017-11-01` && Ticker == `AAPL` && (Close <= 120.0 || isNull(Close))");
  std::cout << t1.stream(true) << '\n';

  auto t2 = table.where(importDate == "2017-11-01" && ticker == "AAPL" &&
      (close <= 120.0 || close.isNull()));
  std::cout << t2.stream(true) << '\n';

  std::vector<std::string> importDateData = {"2017-11-01", "2017-11-01", "2017-11-01"};
  std::vector<std::string> tickerData = {"AAPL", "AAPL", "AAPL"};
  std::vector<double> openData = {22.1, 26.8, 31.5};
  std::vector<double> closeData = {23.5, 24.2, 26.7};
  std::vector<int64_t> volData = {100000, 250000, 19000};

  const TableHandle *tables[] = {&t1, &t2};
  for (const auto *t : tables) {
    compareTable(
        *t,
        "ImportDate", importDateData,
        "Ticker", tickerData,
        "Open", openData,
        "Close", closeData,
        "Volume", volData
        );
  }
}
}  // namespace deephaven::client::tests
