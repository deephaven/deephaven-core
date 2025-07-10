//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class ViewTest {
  [Fact]
  public void View() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    // literal strings
    using var t1 = table.LastBy("Ticker").View("Ticker", "Close", "Volume");

    var expected = new TableMaker();
    expected.AddColumn("Ticker", ["XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA", "T"]);
    expected.AddColumn("Close", [53.8, 88.5, 38.7, 453, 26.7, 544.9, 13.4]);
    expected.AddColumn("Volume", [(Int64)87000, 6060842, 138000, 138000000, 19000, 48300, 1500]);

    TableComparer.AssertSame(expected, t1);
  }
}
