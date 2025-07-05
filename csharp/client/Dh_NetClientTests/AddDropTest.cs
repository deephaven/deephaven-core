//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;
using Xunit.Abstractions;

namespace Deephaven.Dh_NetClientTests;

public class AddDropTest(ITestOutputHelper output) {
  [Fact]
  public void TestDropSomeColumns() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    var t = table.Update("II = ii").Where("Ticker == `AAPL`");
    var cn = ctx.ColumnNames;
    var t2 = t.DropColumns(cn.ImportDate, cn.Ticker, cn.Open, cn.Close);
    output.WriteLine(t2.ToString(true));

    var expected = new TableMaker();
    expected.AddColumn("Volume", [(Int64)100000, 250000, 19000]);
    expected.AddColumn("II", [(Int64)5, 6, 7]);

    TableComparer.AssertSame(expected, t2);
  }
}
