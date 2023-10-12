/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::TableHandleManager;
using deephaven::client::TableHandle;

namespace deephaven::client::tests {
TEST_CASE("View", "[View]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  // literal strings
  auto t1 = table.LastBy("Ticker").View("Ticker", "Close", "Volume");
  std::cout << t1.Stream(true) << '\n';

  // Symbolically
  auto ticker = table.GetStrCol("Ticker");
  auto close = table.GetNumCol("Close");
  auto volume = table.GetNumCol("Volume");
  auto t2 = table.LastBy(ticker).View(ticker, close, volume);
  std::cout << t2.Stream(true) << '\n';

  std::vector<std::string> ticker_data = {"XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA", "T"};
  std::vector<double> close_data = {53.8, 88.5, 38.7, 453, 26.7, 544.9, 13.4};
  std::vector<int64_t> vol_data = {87000, 6060842, 138000, 138000000, 19000, 48300, 1500};

  const TableHandle *tables[] = {&t1, &t2};
  for (const auto *t : tables) {
    CompareTable(
        *t,
        "Ticker", ticker_data,
        "Close", close_data,
        "Volume", vol_data
    );
  }
}
}  // namespace deephaven::client::tests
