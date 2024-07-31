using Deephaven.DeephavenClient;
using Xunit.Abstractions;

namespace Deephaven.DhClientTests;

public class LastByTest {
  private readonly ITestOutputHelper _output;

  public LastByTest(ITestOutputHelper output) {
    _output = output;
  }

  [Fact]
  public void TestLastBy() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;
    var testTable = ctx.TestTable;

    using var lb = testTable.Where("ImportDate == `2017-11-01`")
      .Select("Ticker", "Open", "Close")
      .LastBy("Ticker");

    var tickerData = new[] {
      "XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA"
    };

    var openData = new[] { 50.5, 92.3, 40.1, 681.43, 31.5, 685.3 };
    var closeData = new[] { 53.8, 88.5, 38.7, 453, 26.7, 544.9 };
    var tc = new TableComparer();
    tc.AddColumn("Ticker", tickerData);
    tc.AddColumn("Open", openData);
    tc.AddColumn("Close", closeData);
    tc.AssertEqualTo(lb);
  }
}
