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
TEST_CASE("Head and Tail", "[headtail]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  auto importDate = table.getStrCol("ImportDate");
  auto ticker = table.getStrCol("Ticker");
  auto volume = table.getNumCol("Volume");

  table = table.where(importDate == "2017-11-01");

  auto th = table.head(2).select(ticker, volume);
  auto tt = table.tail(2).select(ticker, volume);

  std::cout << "==== Head(2) ====\n";
  std::cout << th.stream(true) << '\n';
  std::cout << tt.stream(true) << '\n';

  std::vector<std::string> headTickerData = {"XRX", "XRX"};
  std::vector<std::int64_t> headVolumeData = {345000, 87000};

  std::vector<std::string> tailTickerData = {"ZNGA", "ZNGA"};
  std::vector<std::int64_t> tailVolumeData = {46123, 48300};

  compareTable(
      th,
      "Ticker", headTickerData,
      "Volume", headVolumeData
      );

  compareTable(
      tt,
      "Ticker", tailTickerData,
      "Volume", tailVolumeData
      );
}
}  // namespace tests
}  // namespace client
}  // namespace deephaven
