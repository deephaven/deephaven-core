/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/client/client.h"
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/dhcore/container/row_sequence.h"

using deephaven::client::utility::TableMaker;

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
}  // namespace deephaven::client::tests
