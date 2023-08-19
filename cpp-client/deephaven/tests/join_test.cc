/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"

using deephaven::client::TableHandleManager;
using deephaven::client::TableHandle;

namespace deephaven::client::tests {
TEST_CASE("Join", "[join]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  auto import_date = table.GetStrCol("ImportDate");
  auto ticker = table.GetStrCol("Ticker");
  auto volume = table.GetNumCol("Volume");
  auto close = table.GetNumCol("Close");

  table = table.Where(import_date == "2017-11-01");
  auto last_close = table.LastBy(ticker);
  auto avg_view = table.View(ticker, volume).AvgBy(ticker);

  auto joined = last_close.NaturalJoin(avg_view,
      {ticker},
      {volume.as("ADV")});

  auto adv = joined.GetNumCol("ADV");
  auto filtered = joined.Select(ticker, close, adv);
  std::cout << filtered.Stream(true) << '\n';

  std::vector<std::string> ticker_data = {"XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA"};
  std::vector<double> close_data = {53.8, 88.5, 38.7, 453, 26.7, 544.9};
  std::vector<double> adv_data = {216000, 6060842, 138000, 138000000, 123000, 47211.50};

  CompareTable(
      filtered,
      "Ticker", ticker_data,
      "Close", close_data,
      "ADV", adv_data
  );
}
}  // namespace deephaven::client::tests
