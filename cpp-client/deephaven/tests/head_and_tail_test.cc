/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"

using deephaven::client::TableHandleManager;
using deephaven::client::TableHandle;

namespace deephaven::client::tests {
TEST_CASE("Head and Tail", "[headtail]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  auto import_date = table.GetStrCol("ImportDate");
  auto ticker = table.GetStrCol("Ticker");
  auto volume = table.GetNumCol("Volume");

  table = table.Where(import_date == "2017-11-01");

  auto th = table.Head(2).Select(ticker, volume);
  auto tt = table.Tail(2).Select(ticker, volume);

  std::cout << "==== Head(2) ====\n";
  std::cout << th.Stream(true) << '\n';
  std::cout << tt.Stream(true) << '\n';

  std::vector<std::string> head_ticker_data = {"XRX", "XRX"};
  std::vector<std::int64_t> head_volume_data = {345000, 87000};

  std::vector<std::string> tail_ticker_data = {"ZNGA", "ZNGA"};
  std::vector<std::int64_t> tail_volume_data = {46123, 48300};

  CompareTable(
      th,
      "Ticker", head_ticker_data,
      "Volume", head_volume_data
  );

  CompareTable(
      tt,
      "Ticker", tail_ticker_data,
      "Volume", tail_volume_data
  );
}
}  // namespace deephaven::client::tests
