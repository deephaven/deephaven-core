using Deephaven.DeephavenClient;
using System;
using Deephaven.DeephavenClient.Utility;

namespace Deephaven.DhClientTests;

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

    var tickerData = new[] { "ZNGA", "ZNGA", "XYZZY", "XRX", "XRX"};
    var openData = new[] { 541.2, 685.3, 92.3, 50.5, 83.1 };
    var volData = new Int64[] { 46123, 48300, 6060842, 87000, 345000 };

    var tc = new TableComparer();
    tc.AddColumn("Ticker", tickerData);
    tc.AddColumn("Open", openData);
    tc.AddColumn("Volume", volData);
    tc.AssertEqualTo(table1);
  }

  [Fact]
  public void SortTempTable() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var intData0 = new []{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    var intData1 = new []{ 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7};
    var intData2 = new []{ 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3};
    var intData3 = new []{ 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1};

    var maker = new TableMaker();
    maker.AddColumn("IntValue0", intData0);
    maker.AddColumn("IntValue1", intData1);
    maker.AddColumn("IntValue2", intData2);
    maker.AddColumn("IntValue3", intData3);

    var temp_table = maker.MakeTable(ctx.Client.Manager);

    var sorted = temp_table.Sort(SortPair.Descending("IntValue3"), SortPair.Ascending("IntValue2"));

    var sid0 = new[] { 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7};
    var sid1 = new[] { 4, 4, 5, 5, 6, 6, 7, 7, 0, 0, 1, 1, 2, 2, 3, 3};
    var sid2 = new[] { 2, 2, 2, 2, 3, 3, 3, 3, 0, 0, 0, 0, 1, 1, 1, 1};
    var sid3 = new[] { 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0};

    var tc = new TableComparer();
    tc.AddColumn("IntValue0", sid0);
    tc.AddColumn("IntValue1", sid1);
    tc.AddColumn("IntValue2", sid2);
    tc.AddColumn("IntValue3", sid3);
    tc.AssertEqualTo(sorted);
  }
}
