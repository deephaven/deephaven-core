/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */
#include "tests/catch.hpp"
#include "tests/test_util.h"

using deephaven::client::highlevel::TableHandleManager;
using deephaven::client::highlevel::TableHandle;

namespace deephaven {
namespace client {
namespace tests {
TEST_CASE("Join", "[join]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  auto importDate = table.getStrCol("ImportDate");
  auto ticker = table.getStrCol("Ticker");
  auto volume = table.getNumCol("Volume");
  auto close = table.getNumCol("Close");

  table = table.where(importDate == "2017-11-01");
  auto lastClose = table.lastBy(ticker);
  auto avgView = table.view(ticker, volume).avgBy(ticker);

  auto joined = lastClose.naturalJoin(avgView,
      { ticker },
      { volume.as("ADV") });

  auto adv = joined.getNumCol("ADV");
  auto filtered = joined.select(ticker, close, adv);
  std::cout << filtered.stream(true) << '\n';

  std::vector<std::string> tickerData = {"XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA"};
  std::vector<double> closeData = {53.8, 88.5, 38.7, 453, 26.7, 544.9};
  std::vector<double> advData = {216000, 6060842, 138000, 138000000, 123000, 47211.50};

  compareTable(
      filtered,
      "Ticker", tickerData,
      "Close", closeData,
      "ADV", advData
      );
}
}  // namespace tests
}  // namespace client
}  // namespace deephaven
