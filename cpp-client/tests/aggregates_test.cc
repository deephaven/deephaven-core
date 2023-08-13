/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::AggAvg;
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
using deephaven::dhcore::DeephavenConstants;
using deephaven::dhcore::utility::Streamf;
using deephaven::dhcore::utility::Stringf;

namespace deephaven::client::tests {
TEST_CASE("Various Aggregates", "[aggregates]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  auto import_date = table.GetStrCol("ImportDate");
  auto ticker = table.GetStrCol("Ticker");
  auto close = table.GetNumCol("Close");
  table = table.Where(import_date == "2017-11-01");
  auto znga_table = table.Where(ticker == "ZNGA");

  std::cout << znga_table.HeadBy(5, ticker).Stream(true) << '\n';
  std::cout << znga_table.TailBy(5, ticker).Stream(true) << '\n';

  auto agg_table1 = znga_table.View(close)
      .By(AggregateCombo::Create({
          Aggregate::Avg("AvgClose=Close"),
          Aggregate::Sum("SumClose=Close"),
          Aggregate::Min("MinClose=Close"),
          Aggregate::Max("MaxClose=Close"),
          Aggregate::Count("Count")}));
  std::cout << agg_table1.Stream(true) << '\n';

  auto agg_table2 = znga_table.View(close)
      .By(aggCombo({
          AggAvg(close.as("AvgClose")),
          aggSum(close.as("SumClose")),
          aggMin(close.as("MinClose")),
          aggMax(close.as("MaxClose")),
          aggCount("Count")}));
  std::cout << agg_table2.Stream(true) << '\n';

  std::vector<std::string> ticker_data = {"AAPL", "AAPL", "AAPL"};
  std::vector<double> avg_close_data = {541.55};
  std::vector<double> sum_close_data = {1083.1};
  std::vector<double> min_close_data = {538.2};
  std::vector<double> max_close_data = {544.9};
  std::vector<int64_t> count_data = {2};

  const TableHandle *tables[] = {&agg_table1, &agg_table2};
  for (const auto *t : tables) {
    CompareTable(
        *t,
        "AvgClose", avg_close_data,
        "SumClose", sum_close_data,
        "MinClose", min_close_data,
        "MaxClose", max_close_data,
        "Count", count_data
    );
  }
}
}  // namespace deephaven::client::tests
