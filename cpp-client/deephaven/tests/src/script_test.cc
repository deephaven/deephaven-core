/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {
TEST_CASE("Script session error", "[script]") {
  auto client = TableMakerForTests::CreateClient(ClientOptions().SetSessionType(""));

  auto thm = client.GetManager();
  const char *script = "from deephaven import empty_table";
  CHECK_THROWS_WITH(thm.RunScript(script), Catch::Contains("Client was created without specifying a script language"));
}

TEST_CASE("Script execution", "[script]") {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();

  const char *script = R"xxx(
from deephaven import empty_table
mytable = empty_table(16).update(["intData = (int)(ii - 8)", "longData = (long)((ii - 8) * 100)"])
)xxx";

  thm.RunScript(script);
  auto t = thm.FetchTable("mytable");

  std::cout << t.Stream(true) << '\n';

  std::vector<int32_t> int_data;
  std::vector<int64_t> long_data;

  const int start_value = -8;
  const int end_value = 8;
  for (auto i = start_value; i != end_value; ++i) {
    int_data.push_back(i);
    long_data.push_back(i * 100);
  }

  TableMaker expected;
  expected.AddColumn("intData", int_data);
  expected.AddColumn("longData", long_data);
  TableComparerForTests::Compare(expected, t);
}

TEST_CASE("Table operations do not need a console", "[script]") {
  // Never runs a script, so no console should be needed.
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();

  auto t = thm.EmptyTable(10);
  CHECK(t.NumRows() == 10);
}

TEST_CASE("Invalid session type does not cause error until RunScript", "[script]") {
  auto client = TableMakerForTests::CreateClient(ClientOptions().SetSessionType("invalid-session-type-for-test"));
  auto thm = client.GetManager();

  auto t = thm.EmptyTable(10);
  CHECK(t.NumRows() == 10);

  CHECK_THROWS_WITH(
    thm.RunScript("from deephaven import empty_table\nt1 = empty_table(3)"),
    Catch::Matchers::Contains("'invalid-session-type-for-test' is not supported"));
}

TEST_CASE("Variables persist across scripts", "[script]") {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();

  thm.RunScript("from deephaven import empty_table\nt1 = empty_table(3)");
  auto t1 = thm.FetchTable("t1");
  CHECK(t1.NumRows() == 3);

  // The second script sees t1, so lazily starting the console did not lose
  // the query scope. This does not prove the console ticket was reused: the
  // server wraps one shared ScriptSession in a new DelegatingScriptSession per
  // StartConsole, so the scope would survive a second console anyway.
  thm.RunScript("t2 = t1.update([\"x = ii\"])");
  auto t2 = thm.FetchTable("t2");
  CHECK(t2.NumRows() == 3);
}
}  // namespace deephaven::client::tests
