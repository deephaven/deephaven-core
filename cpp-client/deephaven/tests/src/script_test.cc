/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::client::tests {
TEST_CASE("Script session error", "[script]") {
  auto client = TableMakerForTests::CreateClient(ClientOptions().SetSessionType(""));

  auto thm = client.GetManager();
  const char *script = "from deephaven import empty_table";
  CHECK_THROWS_WITH(thm.RunScript(script), Catch::Contains("Client was created without specifying a script language"));
}

TEST_CASE("Script execution", "[script]") {
  std::vector<int32_t> int_data;
  std::vector<int64_t> long_data;

  const int start_value = -8;
  const int end_value = 8;
  for (auto i = start_value; i != end_value; ++i) {
    int_data.push_back(i);
    long_data.push_back(i * 100);
  }

  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();

  const char *script = R"xxx(
from deephaven import empty_table
mytable = empty_table(16).update(["intData = (int)(ii - 8)", "longData = (long)((ii - 8) * 100)"])
)xxx";

  thm.RunScript(script);
  auto t = thm.FetchTable("mytable");

  std::cout << t.Stream(true) << '\n';

  CompareTable(
      t,
      "intData", int_data,
      "longData", long_data
  );
}
}  // namespace deephaven::client::tests
