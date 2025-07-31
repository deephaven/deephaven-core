//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;
using Xunit.Abstractions;

namespace Deephaven.Dh_NetClientTests;

public class LastByTest(ITestOutputHelper output) {
  private readonly ITestOutputHelper _output = output;

  [Fact]
  public void TestLastBy() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var testTable = ctx.TestTable;

    using var lb = testTable.Where("ImportDate == `2017-11-01`")
      .Select("Ticker", "Open", "Close")
      .LastBy("Ticker");

    var tickerData = new[] {
      "XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA"
    };

    var openData = new[] { 50.5, 92.3, 40.1, 681.43, 31.5, 685.3 };
    var closeData = new[] { 53.8, 88.5, 38.7, 453, 26.7, 544.9 };
    var expected = new TableMaker();
    expected.AddColumn("Ticker", tickerData);
    expected.AddColumn("Open", openData);
    expected.AddColumn("Close", closeData);

    TableComparer.AssertSame(expected, lb);
  }
}
