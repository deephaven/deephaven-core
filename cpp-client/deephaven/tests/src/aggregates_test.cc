/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::utility::TableMaker;

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

  TableMaker expected;
  expected.AddColumn<double>("AvgClose", {541.55});
  expected.AddColumn<double>("SumClose", {1083.1});
  expected.AddColumn<double>("MinClose", {538.2});
  expected.AddColumn<double>("MaxClose", {544.9});
  expected.AddColumn<int64_t>("Count", {2});

  TableComparerForTests::Compare(expected, agg_table);
}
}  // namespace deephaven::client::tests
