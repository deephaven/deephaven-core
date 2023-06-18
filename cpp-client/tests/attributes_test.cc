/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include <arrow/flight/client.h>
#include <arrow/flight/client_auth.h>
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/client.h"
#include "deephaven/dhcore/utility/utility.h"

#include <iostream>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/array.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compare.h>
#include <arrow/record_batch.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>

namespace deephaven::client::tests {
TEST_CASE("TableHandle Attributes", "[attributes]") {
  auto client = TableMakerForTests::createClient();
  auto thm = client.getManager();
  int64_t numRows = 37;
  auto t = thm.emptyTable(numRows).update("II = ii");
  CHECK(t.numRows() == numRows);
  CHECK(t.isStatic());
}

TEST_CASE("TableHandle Dynamic Attributes", "[attributes]") {
  auto client = TableMakerForTests::createClient();
  auto thm = client.getManager();
  auto t = thm.timeTable(0, 1'000'000'000).update("II = ii");
  CHECK(!t.isStatic());
}

TEST_CASE("TableHandle Created by DoPut", "[attributes]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();
  CHECK(table.isStatic());
  // The columns all have the same size, so look at the source data for any one of them and get its size
  auto expectedSize = int64_t(tm.columnData().importDate().size());
  CHECK(table.numRows() == expectedSize);
}
}  // namespace deephaven::client::tests
