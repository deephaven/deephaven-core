/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"

namespace deephaven::client::tests {
TEST_CASE("TableHandle Attributes", "[attributes]") {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();
  int64_t num_rows = 37;
  auto t = thm.EmptyTable(num_rows).Update("II = ii");
  CHECK(t.NumRows() == num_rows);
  CHECK(t.IsStatic());
}

TEST_CASE("TableHandle Dynamic Attributes", "[attributes]") {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();
  auto t = thm.TimeTable(1'000'000'000).Update("II = ii");
  CHECK(!t.IsStatic());
}

TEST_CASE("TableHandle Created By DoPut", "[attributes]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();
  CHECK(table.IsStatic());
  // The columns all have the same size, so look at the source data for any one of them and get its size
  auto expected_size = static_cast<int64_t>(tm.ColumnData().ImportDate().size());
  CHECK(expected_size == table.NumRows());
}
}  // namespace deephaven::client::tests
