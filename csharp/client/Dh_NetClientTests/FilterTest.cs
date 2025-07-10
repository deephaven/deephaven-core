//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;
using Xunit.Abstractions;

namespace Deephaven.Dh_NetClientTests;

public class FilterTest(ITestOutputHelper output) {
  [Fact]
  public void TestFilterATable() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    var t1 = table.Where(
      "ImportDate == `2017-11-01` && Ticker == `AAPL` && (Close <= 120.0 || isNull(Close))");
    output.WriteLine(t1.ToString(true));

    var expected = new TableMaker();
    expected.AddColumn("ImportDate", ["2017-11-01", "2017-11-01", "2017-11-01"]);
    expected.AddColumn("Ticker", ["AAPL", "AAPL", "AAPL"]);
    expected.AddColumn("Open", [22.1, 26.8, 31.5]);
    expected.AddColumn("Close", [23.5, 24.2, 26.7]);
    expected.AddColumn("Volume", [(Int64)100000, 250000, 19000]);

    TableComparer.AssertSame(expected, t1);
  }
}
