/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"

#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {
TEST_CASE("Sort demo Table", "[sort]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  // Limit by date and ticker
  auto filtered = table.Where("ImportDate == `2017-11-01`")
      .Where("Ticker >= `X`")
      .Select("Ticker", "Open", "Volume");
  std::cout << filtered.Stream(true) << '\n';

  auto table1 = filtered.Sort(SortPair::Descending("Ticker"), SortPair::Ascending("Volume"));

  TableMaker expected;
  expected.AddColumn<std::string>("Ticker", {"ZNGA", "ZNGA", "XYZZY", "XRX", "XRX"});
  expected.AddColumn<double>("Open", {541.2, 685.3, 92.3, 50.5, 83.1});
  expected.AddColumn<int64_t>("Volume", {46123, 48300, 6060842, 87000, 345000});
  TableComparerForTests::Compare(expected, table1);
}

TEST_CASE("Sort temp Table", "[sort]") {
  auto tm = TableMakerForTests::Create();

  TableMaker maker;
  maker.AddColumn<int32_t>("IntValue0", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
  maker.AddColumn<int32_t>("IntValue1", {0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7});
  maker.AddColumn<int32_t>("IntValue2", {0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3});
  maker.AddColumn<int32_t>("IntValue3", {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1});

  auto temp_table = maker.MakeTable(tm.Client().GetManager());

  auto sorted = temp_table.Sort(SortPair::Descending("IntValue3"), SortPair::Ascending("IntValue2"));

  TableMaker expected;
  expected.AddColumn<int32_t>("IntValue0", {8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7});
  expected.AddColumn<int32_t>("IntValue1", {4, 4, 5, 5, 6, 6, 7, 7, 0, 0, 1, 1, 2, 2, 3, 3});
  expected.AddColumn<int32_t>("IntValue2", {2, 2, 2, 2, 3, 3, 3, 3, 0, 0, 0, 0, 1, 1, 1, 1});
  expected.AddColumn<int32_t>("IntValue3", {1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0});
  TableComparerForTests::Compare(expected, sorted);
}
}  // namespace deephaven::client::tests
