//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class SortTest {
  [Fact]
  public void SortDemoTable() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var testTable = ctx.TestTable;

    // Limit by date and ticker
    var filtered = testTable.Where("ImportDate == `2017-11-01`")
      .Where("Ticker >= `X`")
      .Select("Ticker", "Open", "Volume");

    var table1 = filtered.Sort(SortPair.Descending("Ticker"), SortPair.Ascending("Volume"));

    var expected = new TableMaker();
    expected.AddColumn("Ticker", ["ZNGA", "ZNGA", "XYZZY", "XRX", "XRX"]);
    expected.AddColumn("Open", [541.2, 685.3, 92.3, 50.5, 83.1]);
    expected.AddColumn("Volume", [(Int64)46123, 48300, 6060842, 87000, 345000]);

    TableComparer.AssertSame(expected, table1);
  }

  [Fact]
  public void SortTempTable() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var maker = new TableMaker();
    maker.AddColumn("IntValue0", [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    maker.AddColumn("IntValue1", [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7]);
    maker.AddColumn("IntValue2", [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3]);
    maker.AddColumn("IntValue3", [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1]);

    var tempTable = maker.MakeTable(ctx.Client.Manager);

    var sorted = tempTable.Sort(SortPair.Descending("IntValue3"), SortPair.Ascending("IntValue2"));

    var expected = new TableMaker();
    expected.AddColumn("IntValue0", [8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7]);
    expected.AddColumn("IntValue1", [4, 4, 5, 5, 6, 6, 7, 7, 0, 0, 1, 1, 2, 2, 3, 3]);
    expected.AddColumn("IntValue2", [2, 2, 2, 2, 3, 3, 3, 3, 0, 0, 0, 0, 1, 1, 1, 1]);
    expected.AddColumn("IntValue3", [1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0]);

    TableComparer.AssertSame(expected, sorted);
  }
}
