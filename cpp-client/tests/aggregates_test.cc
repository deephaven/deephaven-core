/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::aggAvg;
using deephaven::client::aggSum;
using deephaven::client::aggMin;
using deephaven::client::aggMax;
using deephaven::client::aggCount;
using deephaven::client::aggCombo;
using deephaven::client::Aggregate;
using deephaven::client::AggregateCombo;
using deephaven::client::TableHandleManager;
using deephaven::client::TableHandle;
using deephaven::client::SortPair;
using deephaven::client::DeephavenConstants;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;

namespace deephaven::client::tests {
TEST_CASE("Various aggregates", "[aggregates]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  auto importDate = table.getStrCol("ImportDate");
  auto ticker = table.getStrCol("Ticker");
  auto close = table.getNumCol("Close");
  table = table.where(importDate == "2017-11-01");
  auto zngaTable = table.where(ticker == "ZNGA");

  std::cout << zngaTable.headBy(5, ticker).stream(true) << '\n';
  std::cout << zngaTable.tailBy(5, ticker).stream(true) << '\n';

  auto aggTable1 = zngaTable.view(close)
      .by(AggregateCombo::create({
          Aggregate::avg("AvgClose=Close"),
          Aggregate::sum("SumClose=Close"),
          Aggregate::min("MinClose=Close"),
          Aggregate::max("MaxClose=Close"),
          Aggregate::count("Count")}));
  std::cout << aggTable1.stream(true) << '\n';

  auto aggTable2 = zngaTable.view(close)
      .by(aggCombo({
          aggAvg(close.as("AvgClose")),
          aggSum(close.as("SumClose")),
          aggMin(close.as("MinClose")),
          aggMax(close.as("MaxClose")),
          aggCount("Count")}));
  std::cout << aggTable2.stream(true) << '\n';

  std::vector<std::string> tickerData = {"AAPL", "AAPL", "AAPL"};
  std::vector<double> avgCloseData = {541.55};
  std::vector<double> sumCloseData = {1083.1};
  std::vector<double> minCloseData = {538.2};
  std::vector<double> maxCloseData = {544.9};
  std::vector<int64_t> countData = {2};

  const TableHandle *tables[] = {&aggTable1, &aggTable2};
  for (const auto *t : tables) {
    compareTable(
        *t,
        "AvgClose", avgCloseData,
        "SumClose", sumCloseData,
        "MinClose", minCloseData,
        "MaxClose", maxCloseData,
        "Count", countData
        );
  }
}
}  // namespace deephaven::client::tests
