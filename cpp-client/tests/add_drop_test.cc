/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"

using deephaven::client::utility::streamf;
using deephaven::client::NumericExpression;

namespace deephaven::client::tests {

TEST_CASE("Drop all columns", "[adddrop]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();
  auto ticker = table.getStrCol("Ticker");
  auto t = table.update("II = ii").where(ticker == "AAPL");
  const auto &cn = tm.columnNames();
  auto t2 = t.dropColumns(cn.importDate(), cn.ticker(), cn.open(), cn.close());
  std::cout << t2.stream(true) << '\n';

  std::vector<int64_t> volData = {100000, 250000, 19000};
  std::vector<int64_t> iiData = {5, 6, 7};

  compareTable(
      t2,
      "Volume", volData,
      "II", iiData
  );
}
}  // namespace deephaven::client::tests
