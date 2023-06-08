/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/client.h"
#include "deephaven/dhcore/utility/utility.h"

#include <iostream>
using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {
TEST_CASE("Script session error", "[script]") {
  auto client = TableMakerForTests::createClient(ClientOptions().setSessionType(""));

  auto thm = client.getManager();
  const char *script = "from deephaven import empty_table";
  CHECK_THROWS_WITH(thm.runScript(script), Catch::Contains("Client was created without specifying a script language"));
}

TEST_CASE("Script execution", "[script]") {
  std::vector<int32_t> intData;
  std::vector<int64_t> longData;

  const int startValue = -8;
  const int endValue = 8;
  for (auto i = startValue; i != endValue; ++i) {
    intData.push_back(i);
    longData.push_back(i * 100);
  }

  auto client = TableMakerForTests::createClient();
  auto thm = client.getManager();

  const char *script = R"xxx(
from deephaven import empty_table
mytable = empty_table(16).update(["intData = (int)(ii - 8)", "longData = (long)((ii - 8) * 100)"])
)xxx";

  thm.runScript(script);
  auto t = thm.fetchTable("mytable");

  std::cout << t.stream(true) << '\n';

  compareTable(
      t,
      "intData", intData,
      "longData", longData
  );
}
}  // namespace deephaven::client::tests
