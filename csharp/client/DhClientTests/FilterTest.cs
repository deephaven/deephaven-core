using Deephaven.DeephavenClient.Interop;
using System;
using Deephaven.DeephavenClient;
using Deephaven.DeephavenClient.Utility;
using Xunit.Abstractions;

namespace Deephaven.DhClientTests;

public class FilterTest {
  private readonly ITestOutputHelper _output;

  public FilterTest(ITestOutputHelper output) {
    _output = output;
  }

  [Fact]
  public void TestFilterATable() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    var t1 = table.Where(
      "ImportDate == `2017-11-01` && Ticker == `AAPL` && (Close <= 120.0 || isNull(Close))");
    _output.WriteLine(t1.ToString(true));

    var importDateData = new[] { "2017-11-01", "2017-11-01", "2017-11-01"};
    var tickerData = new []{ "AAPL", "AAPL", "AAPL"};
    var openData = new[] { 22.1, 26.8, 31.5 };
    var closeData = new[] { 23.5, 24.2, 26.7 };
    var volData = new Int64[] { 100000, 250000, 19000 };

    var tc = new TableComparer();
    tc.AddColumn("ImportDate", importDateData);
    tc.AddColumn("Ticker", tickerData);
    tc.AddColumn("Open", openData);
    tc.AddColumn("Close", closeData);
    tc.AddColumn("Volume", volData);
    tc.AssertEqualTo(t1);
  }
}
