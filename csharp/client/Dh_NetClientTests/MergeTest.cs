//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class MergeTest {
  [Fact]
  public void TestMerge() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var testTable = ctx.TestTable;

    using var table = testTable.Where("ImportDate == `2017-11-01`");

    // Run a merge by fetching two tables and them merging them
    var aaplTable = table.Where("Ticker == `AAPL`").Tail(10);
    var zngaTable = table.Where("Ticker == `ZNGA`").Tail(10);

    var merged = aaplTable.Merge(zngaTable);

    var expected = new TableMaker();
    expected.AddColumn("ImportDate", [
      "2017-11-01", "2017-11-01", "2017-11-01",
      "2017-11-01", "2017-11-01"
    ]);
    expected.AddColumn("Ticker", ["AAPL", "AAPL", "AAPL", "ZNGA", "ZNGA"]);
    expected.AddColumn("Open", [22.1, 26.8, 31.5, 541.2, 685.3]);
    expected.AddColumn("Close", [23.5, 24.2, 26.7, 538.2, 544.9]);
    expected.AddColumn("Volume", [(Int64)100000, 250000, 19000, 46123, 48300]);

    TableComparer.AssertSame(expected, merged);
  }
}
