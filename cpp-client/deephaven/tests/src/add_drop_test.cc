/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"

using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {

TEST_CASE("Drop some columns", "[adddrop]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();
  auto t = table.Update("II = ii").Where("Ticker == `AAPL`");
  const auto &cn = tm.ColumnNames();
  auto t2 = t.DropColumns(cn.ImportDate(), cn.Ticker(), cn.Open(), cn.Close());
  std::cout << t2.Stream(true) << '\n';

  TableMaker expected;
  expected.AddColumn<int64_t>("Volume", {100000, 250000, 19000});
  expected.AddColumn<int64_t>("II", {5, 6, 7});

  TableComparerForTests::Compare(expected, t2);
}
}  // namespace deephaven::client::tests
