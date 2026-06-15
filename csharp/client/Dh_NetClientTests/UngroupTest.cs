//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class UngroupTest {
  [Test]
  public async Task UngroupColumns() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    table = table.Where("ImportDate == `2017-11-01`");

    var byTable = table.Where("Ticker == `AAPL`").View("Ticker", "Close").By("Ticker");
    Console.WriteLine(byTable.ToString(true, true));
    var ungrouped = byTable.Ungroup("Close");
    Console.WriteLine(ungrouped.ToString(true, true));

    {
      var expected = new TableMaker();
      expected.AddColumn("Ticker", ["AAPL"]);
      expected.AddColumn<double[]>("Close", [[23.5, 24.2, 26.7]]);
      await Assert.That(() => TableComparer.AssertSame(expected, byTable)).ThrowsNothing();
    }

    {
      var expected = new TableMaker();
      expected.AddColumn("Ticker", ["AAPL", "AAPL", "AAPL"]);
      expected.AddColumn("Close", [23.5, 24.2, 26.7]);
      await Assert.That(() => TableComparer.AssertSame(expected, ungrouped)).ThrowsNothing();
    }
  }
}
