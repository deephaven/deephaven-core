/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::TableHandleManager;
using deephaven::client::TableHandle;
using deephaven::client::utility::streamf;

namespace deephaven::client::tests {
TEST_CASE("View", "[view]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  // literal strings
  auto t1 = table.lastBy("Ticker").view("Ticker", "Close", "Volume");
  std::cout << t1.stream(true) << '\n';

  // Symbolically
  auto ticker = table.getStrCol("Ticker");
  auto close = table.getNumCol("Close");
  auto volume = table.getNumCol("Volume");
  auto t2 = table.lastBy(ticker).view(ticker, close, volume);
  std::cout << t2.stream(true) << '\n';

  std::vector<std::string> tickerData = {"XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA", "T"};
  std::vector<double> closeData = {53.8, 88.5, 38.7, 453, 26.7, 544.9, 13.4};
  std::vector<int64_t> volData = {87000, 6060842, 138000, 138000000, 19000, 48300, 1500};

  const TableHandle *tables[] = {&t1, &t2};
  for (const auto *t : tables) {
    compareTable(
        *t,
        "Ticker", tickerData,
        "Close", closeData,
        "Volume", volData
        );
  }
}
}  // namespace deephaven::client::tests
