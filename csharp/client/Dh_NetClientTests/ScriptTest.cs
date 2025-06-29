//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class ScriptTest {
  [Fact]
  public void TestScriptSessionError() {
    using var ctx = CommonContextForTests.Create(new ClientOptions().SetSessionType(""));
    var m = ctx.Client.Manager;
    const string script = "from deephaven import empty_table";
    var ex = Record.Exception(() => m.RunScript(script));
    Assert.NotNull(ex);
    Assert.Contains("Client was created without specifying a script language", ex.Message);
  }

  [Fact]
  public void TestScriptExecution() {
    var intData = new List<Int32>();
    var longData = new List<Int64>();

    const int startValue = -8;
    const int endValue = 8;
    for (var i = startValue; i != endValue; ++i) {
      intData.Add(i);
      longData.Add(i * 100);
    }

    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var m = ctx.Client.Manager;

    const string script = """
      from deephaven import empty_table
      mytable = empty_table(16).update(["intData = (int)(ii - 8)", "longData = (long)((ii - 8) * 100)"])
      """;

    m.RunScript(script);
    var t = m.FetchTable("mytable");

    var expected = new TableMaker();
    expected.AddColumn("intData", intData);
    expected.AddColumn("longData", longData);
    TableComparer.AssertSame(expected, t);
  }
}
