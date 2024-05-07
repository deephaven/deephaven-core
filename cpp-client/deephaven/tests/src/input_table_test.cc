/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {
TEST_CASE("Input Table: append", "[input_table]") {
  auto client = TableMakerForTests::CreateClient();
  auto tm = client.GetManager();
  auto source = tm.EmptyTable(3).Update({"A = ii", "B = ii + 100"});
  // No keys, so InputTable will be in append-only mode.
  auto input_table = tm.InputTable(source);

  // expect input_table to be {0, 100}, {1, 101}, {2, 102}
  {
    std::vector<int64_t> a_data = {0, 1, 2};
    std::vector<int64_t> b_data = {100, 101, 102};
    CompareTable(input_table,
        "A", a_data,
        "B", b_data);
  }

  auto table_to_add = tm.EmptyTable(2).Update({"A = ii", "B = ii + 200"});
  input_table.AddTable(table_to_add);

  // Because of append, expect input_table to be {0, 100}, {1, 101}, {2, 102}, {0, 200}, {1, 201}
  {
    std::vector<int64_t> a_data = {0, 1, 2, 0, 1};
    std::vector<int64_t> b_data = {100, 101, 102, 200, 201};
    CompareTable(input_table,
        "A", a_data,
        "B", b_data);
  }
}

TEST_CASE("Input Table: keyed", "[input_table]") {
  auto client = TableMakerForTests::CreateClient();
  auto tm = client.GetManager();
  auto source = tm.EmptyTable(3).Update({"A = ii", "B = ii + 100"});
  // Keys = {"A"}, so InputTable will be in keyed mode
  auto input_table = tm.InputTable(source, {"A"});

  // expect input_table to be {0, 100}, {1, 101}, {2, 102}
  {
    std::vector<int64_t> a_data = {0, 1, 2};
    std::vector<int64_t> b_data = {100, 101, 102};
    CompareTable(input_table,
        "A", a_data,
        "B", b_data);
  }

  auto table_to_add = tm.EmptyTable(2).Update({"A = ii", "B = ii + 200"});
  input_table.AddTable(table_to_add);

  // Because key is "A", expect input_table to be {0, 200}, {1, 201}, {2, 102}
  {
    std::vector<int64_t> a_data = {0, 1, 2};
    std::vector<int64_t> b_data = {200, 201, 102};
    CompareTable(input_table,
        "A", a_data,
        "B", b_data);
  }
}
}  // namespace deephaven::client::tests
