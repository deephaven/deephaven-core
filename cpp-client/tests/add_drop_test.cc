/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"

using deephaven::client::NumericExpression;
using deephaven::dhcore::utility::Streamf;

namespace deephaven::client::tests {

TEST_CASE("Drop all columns", "[adddrop]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();
  auto ticker = table.GetStrCol("Ticker");
  auto t = table.Update("II = ii").Where(ticker == "AAPL");
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
