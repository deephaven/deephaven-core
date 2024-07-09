using Deephaven.DeephavenClient.Interop;
using Deephaven.DeephavenClient;
using Xunit.Abstractions;

namespace Deephaven.DhClientTests;

public class HeadAndTailTest {
  private readonly ITestOutputHelper _output;

  public HeadAndTailTest(ITestOutputHelper output) {
    _output = output;
  }

  [Fact]
  public void TestHeadAndTail() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    table = table.Where("ImportDate == `2017-11-01`");

    var th = table.Head(2).Select("Ticker", "Volume");
    var tt = table.Tail(2).Select("Ticker", "Volume");

    {
      var tickerData = new[] { "XRX", "XRX" };
      var volumeData = new Int64[] { 345000, 87000 };
      var tc = new TableComparer();

      tc.AddColumn("Ticker", tickerData);
      tc.AddColumn("Volume", volumeData);
      tc.AssertEqualTo(th);
    }

    {
      var tickerData = new[] { "ZNGA", "ZNGA" };
      var volumeData = new Int64[] { 46123, 48300 };
      var tc = new TableComparer();

      tc.AddColumn("Ticker", tickerData);
      tc.AddColumn("Volume", volumeData);
      tc.AssertEqualTo(tt);
    }
  }
}
