/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include <cstdint>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/client/client.h"
#include "deephaven/client/utility/table_maker.h"

using deephaven::client::utility::TableMaker;
using Catch::Matchers::StartsWith;

namespace deephaven::client::tests {
TEST_CASE("Group a Table", "[group]") {
  auto tm = TableMakerForTests::Create();

  TableMaker maker;
  maker.AddColumn<std::string>("Type", {
      "Granny Smith",
      "Granny Smith",
      "Gala",
      "Gala",
      "Golden Delicious",
      "Golden Delicious"
  });
  maker.AddColumn<std::string>("Color", {
      "Green", "Green", "Red-Green", "Orange-Green", "Yellow", "Yellow"
  });
  maker.AddColumn<int32_t>("Weight", {
      102, 85, 79, 92, 78, 99
  });
  maker.AddColumn<int32_t>("Calories", {
      53, 48, 51, 61, 46, 57
  });
  auto t1 = maker.MakeTable(tm.Client().GetManager());

  auto grouped = t1.By("Type");

  std::cout << grouped.Stream(true) << '\n';

  TableMaker expected;
  expected.AddColumn<std::string>("Type", {"Granny Smith", "Gala", "Golden Delicious"});
  expected.AddColumn<std::vector<std::string>>("Color", {
      {"Green", "Green"}, {"Red-Green", "Orange-Green"}, {"Yellow", "Yellow"}
  });
  expected.AddColumn<std::vector<int32_t>>("Weight", {
      {102, 85}, {79, 92}, {78, 99}
  });
  expected.AddColumn<std::vector<int32_t>>("Calories", {
      {53, 48}, {51, 61}, {46, 57}
  });
  TableComparerForTests::Compare(expected, grouped);
}

TEST_CASE("Nested lists not supported", "[group]") {
  auto tm = TableMakerForTests::Create();

  TableMaker maker;
  maker.AddColumn<std::vector<std::vector<int32_t>>>("Value", {
      { { 1, 2, 3 } },  // [[1, 2, 3]]
      { { 4, 5 } },  // [[4, 5]]
  });
  auto t = maker.MakeTable(tm.Client().GetManager());
  CHECK_THROWS_WITH(t.ToClientTable(), StartsWith("Nested lists are not currently supported"));
}

TEST_CASE("Test case for group example", "[group]") {
  // This is not really a "test" per se. Instead, it provides the source data that matches the
  // examples we have documented in arrow_array_converter.cc. It was used to single-step through
  // that code and provide the documentation for various steps in that code.
  auto tm = TableMakerForTests::Create();

  TableMaker maker;
  maker.AddColumn<std::optional<std::vector<std::optional<std::string>>>>("Value", {
      { {"a", "b", "c"} },  // [a, b, c]
      {}, // null
      {{}}, // []
      {{"d", "e", "f", {}, "g"}}  // [d, e, f, null, g]
  });
  auto t = maker.MakeTable(tm.Client().GetManager());
  auto ct = t.ToClientTable();
}
}  // namespace deephaven::client::tests
