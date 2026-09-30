//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class AddDropTest {
  [Test]
  public async Task TestDropSomeColumns() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    var t = table.Update("II = ii").Where("Ticker == `AAPL`");
    var cn = ctx.ColumnNames;
    var t2 = t.DropColumns(cn.ImportDate, cn.Ticker, cn.Open, cn.Close);
    Console.WriteLine(t2.ToString(true));

    var expected = new TableMaker();
    expected.AddColumn("Volume", [(Int64)100000, 250000, 19000]);
    expected.AddColumn("II", [(Int64)5, 6, 7]);

    await Assert.That(() => TableComparer.AssertSame(expected, t2)).ThrowsNothing();
  }
}