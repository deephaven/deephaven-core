/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
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

namespace deephaven::client::tests {
TEST_CASE("Various Aggregates", "[aggregates]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  table = table.Where("ImportDate == `2017-11-01`");
  auto znga_table = table.Where("Ticker == `ZNGA`");

  auto agg_table = znga_table.View("Close")
      .By(AggregateCombo::Create({
          Aggregate::Avg("AvgClose=Close"),
          Aggregate::Sum("SumClose=Close"),
          Aggregate::Min("MinClose=Close"),
          Aggregate::Max("MaxClose=Close"),
          Aggregate::Count("Count")}));

  std::vector<std::string> ticker_data = {"AAPL", "AAPL", "AAPL"};
  std::vector<double> avg_close_data = {541.55};
  std::vector<double> sum_close_data = {1083.1};
  std::vector<double> min_close_data = {538.2};
  std::vector<double> max_close_data = {544.9};
  std::vector<int64_t> count_data = {2};

  CompareTable(
      agg_table,
      "AvgClose", avg_close_data,
      "SumClose", sum_close_data,
      "MinClose", min_close_data,
      "MaxClose", max_close_data,
      "Count", count_data
  );
}
}  // namespace deephaven::client::tests
