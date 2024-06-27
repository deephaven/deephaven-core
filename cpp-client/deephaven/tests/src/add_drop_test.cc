/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"

namespace deephaven::client::tests {

TEST_CASE("Drop some columns", "[adddrop]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();
  auto t = table.Update("II = ii").Where("Ticker == `AAPL`");
  const auto &cn = tm.ColumnNames();
  auto t2 = t.DropColumns(cn.ImportDate(), cn.Ticker(), cn.Open(), cn.Close());
  std::cout << t2.Stream(true) << '\n';

  std::vector<int64_t> vol_data = {100000, 250000, 19000};
  std::vector<int64_t> ii_data = {5, 6, 7};

  CompareTable(
      t2,
      "Volume", vol_data,
      "II", ii_data
  );
}
}  // namespace deephaven::client::tests
