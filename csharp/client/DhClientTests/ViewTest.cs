using Deephaven.DeephavenClient;
using Xunit.Abstractions;

namespace Deephaven.DhClientTests;

public class ViewTest {
  [Fact]
  public void View() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    // literal strings
    using var t1 = table.LastBy("Ticker").View("Ticker", "Close", "Volume");

    var tickerData = new[] { "XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA", "T" };
    var closeData = new[] { 53.8, 88.5, 38.7, 453, 26.7, 544.9, 13.4 };
    var volData = new Int64[] { 87000, 6060842, 138000, 138000000, 19000, 48300, 1500 };

    var tc = new TableComparer();
    tc.AddColumn("Ticker", tickerData);
    tc.AddColumn("Close", closeData);
    tc.AddColumn("Volume", volData);
    tc.AssertEqualTo(t1);
  }
}
