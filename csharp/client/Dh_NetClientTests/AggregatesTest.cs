//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class AggregatesTest {
  [Fact]
  public void TestVariousAggregates() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    table = table.Where("ImportDate == `2017-11-01`");
    var zngaTable = table.Where("Ticker == `ZNGA`");

    var aggTable = zngaTable.View("Close")
      .By(AggregateCombo.Create(
        Aggregate.Avg("AvgClose=Close"),
        Aggregate.Sum("SumClose=Close"),
        Aggregate.Min("MinClose=Close"),
        Aggregate.Max("MaxClose=Close"),
        Aggregate.Count("Count")
      ));

    var expected = new TableMaker();
    expected.AddColumn("AvgClose", [541.55]);
    expected.AddColumn("SumClose", [1083.1]);
    expected.AddColumn("MinClose", [538.2]);
    expected.AddColumn("MaxClose", [544.9]);
    expected.AddColumn("Count", [(Int64)2]);

    TableComparer.AssertSame(expected, aggTable);
  }
}
