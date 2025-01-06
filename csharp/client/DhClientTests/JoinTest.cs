using Deephaven.DeephavenClient;
using Deephaven.DeephavenClient.Utility;
using System;
using Xunit.Abstractions;

namespace Deephaven.DhClientTests;

public class JoinTest {
  private readonly ITestOutputHelper _output;

  public JoinTest(ITestOutputHelper output) {
    _output = output;
  }

  [Fact]
  public void TestJoin() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;
    var testTable = ctx.TestTable;

    using var table = testTable.Where("ImportDate == `2017-11-01`");
    using var lastClose = table.LastBy("Ticker");
    using var avgView = table.View("Ticker", "Volume").AvgBy("Ticker");

    using var joined = lastClose.NaturalJoin(avgView, new[] {"Ticker"}, new[]{ "ADV = Volume"});
    using var filtered = joined.Select("Ticker", "Close", "ADV");

    var tickerData = new[] { "XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA" };
    var closeData = new[] { 53.8, 88.5, 38.7, 453, 26.7, 544.9 };
    var advData = new[] { 216000, 6060842, 138000, 138000000, 123000, 47211.50 };

    var tc = new TableComparer();
    tc.AddColumn("Ticker", tickerData);
    tc.AddColumn("Close", closeData);
    tc.AddColumn("ADV", advData);
    tc.AssertEqualTo(filtered);
  }

  [Fact]
  public void TestAj() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;

    TableHandle trades;
    {
      var tickerData = new[] { "AAPL", "AAPL", "AAPL", "IBM", "IBM" };
      var instantDdata = new[] {
        DhDateTime.Parse("2021-04-05T09:10:00-0500"),
        DhDateTime.Parse("2021-04-05T09:31:00-0500"),
        DhDateTime.Parse("2021-04-05T16:00:00-0500"),
        DhDateTime.Parse("2021-04-05T16:00:00-0500"),
        DhDateTime.Parse("2021-04-05T16:30:00-0500")
      };
      var priceData = new[] { 2.5, 3.7, 3.0, 100.50, 110 };
      var sizeData = new[] { 52, 14, 73, 11, 6 };
      using var tableMaker = new TableMaker();
      tableMaker.AddColumn("Ticker", tickerData);
      tableMaker.AddColumn("Timestamp", instantDdata);
      tableMaker.AddColumn("Price", priceData);
      tableMaker.AddColumn("Size", sizeData);
      trades = tableMaker.MakeTable(tm);
    }

    TableHandle quotes;
    {
      var tickerData = new[] { "AAPL", "AAPL", "IBM", "IBM", "IBM" };
      var timeStampData = new[] {
        DhDateTime.Parse("2021-04-05T09:11:00-0500"),
        DhDateTime.Parse("2021-04-05T09:30:00-0500"),
        DhDateTime.Parse("2021-04-05T16:00:00-0500"),
        DhDateTime.Parse("2021-04-05T16:30:00-0500"),
        DhDateTime.Parse("2021-04-05T17:00:00-0500")
      };
      var bidData = new[] { 2.5, 3.4, 97, 102, 108 };
      var bidSizeData = new[] { 10, 20, 5, 13, 23 };
      var askData = new[] { 2.5, 3.4, 105, 110, 111 };
      var askSizeData = new[] { 83, 33, 47, 15, 5 };
      var tableMaker = new TableMaker();
      tableMaker.AddColumn("Ticker", tickerData);
      tableMaker.AddColumn("Timestamp", timeStampData);
      tableMaker.AddColumn("Bid", bidData);
      tableMaker.AddColumn("BidSize", bidSizeData);
      tableMaker.AddColumn("Ask", askData);
      tableMaker.AddColumn("AskSize", askSizeData);
      quotes = tableMaker.MakeTable(tm);
    }

    using var result = trades.Aj(quotes, new[] { "Ticker", "Timestamp" });

    // Expected data
    {
      var tickerData = new[] { "AAPL", "AAPL", "AAPL", "IBM", "IBM" };
      var timestampData = new[] {
        DhDateTime.Parse("2021-04-05T09:10:00-0500"),
        DhDateTime.Parse("2021-04-05T09:31:00-0500"),
        DhDateTime.Parse("2021-04-05T16:00:00-0500"),
        DhDateTime.Parse("2021-04-05T16:00:00-0500"),
        DhDateTime.Parse("2021-04-05T16:30:00-0500")
      };
      var priceData = new[] { 2.5, 3.7, 3.0, 100.50, 110 };
      var sizeData = new[] { 52, 14, 73, 11, 6 };
      var bidData = new double?[] { null, 3.4, 3.4, 97, 102 };
      var bidSizeData = new int?[] { null, 20, 20, 5, 13 };
      var askData = new double?[] { null, 3.4, 3.4, 105, 110 };
      var askSizeData = new int?[] { null, 33, 33, 47, 15 };

      var tc = new TableComparer();
      tc.AddColumn("Ticker", tickerData);
      tc.AddColumn("Timestamp", timestampData);
      tc.AddColumn("Price", priceData);
      tc.AddColumn("Size", sizeData);
      tc.AddColumn("Bid", bidData);
      tc.AddColumn("BidSize", bidSizeData);
      tc.AddColumn("Ask", askData);
      tc.AddColumn("AskSize", askSizeData);
      tc.AssertEqualTo(result);
    }
  }

  [Fact]
  public void TestRaj() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;

    TableHandle trades;
    {
      var tickerData = new[] { "AAPL", "AAPL", "AAPL", "IBM", "IBM" };
      var instantData = new[] {
        DhDateTime.Parse("2021-04-05T09:10:00-0500"),
        DhDateTime.Parse("2021-04-05T09:31:00-0500"),
        DhDateTime.Parse("2021-04-05T16:00:00-0500"),
        DhDateTime.Parse("2021-04-05T16:00:00-0500"),
        DhDateTime.Parse("2021-04-05T16:30:00-0500")
      };
      var priceData = new[] { 2.5, 3.7, 3.0, 100.50, 110 };
      var sizeData = new[] { 52, 14, 73, 11, 6 };
      var tableMaker = new TableMaker();
      tableMaker.AddColumn("Ticker", tickerData);
      tableMaker.AddColumn("Timestamp", instantData);
      tableMaker.AddColumn("Price", priceData);
      tableMaker.AddColumn("Size", sizeData);
      trades = tableMaker.MakeTable(tm);
    }

    TableHandle quotes;
    {
      var tickerData = new[] { "AAPL", "AAPL", "IBM", "IBM", "IBM"};
      var timestampData = new[] {
        DhDateTime.Parse("2021-04-05T09:11:00-0500"),
        DhDateTime.Parse("2021-04-05T09:30:00-0500"),
        DhDateTime.Parse("2021-04-05T16:00:00-0500"),
        DhDateTime.Parse("2021-04-05T16:30:00-0500"),
        DhDateTime.Parse("2021-04-05T17:00:00-0500")
      };
      var bidData = new[] { 2.5, 3.4, 97, 102, 108 };
      var bidSizeData = new[] { 10, 20, 5, 13, 23 };
      var askData = new[] { 2.5, 3.4, 105, 110, 111 };
      var askSizeData = new[] { 83, 33, 47, 15, 5 };
      var tableMaker = new TableMaker();
      tableMaker.AddColumn("Ticker", tickerData);
      tableMaker.AddColumn("Timestamp", timestampData);
      tableMaker.AddColumn("Bid", bidData);
      tableMaker.AddColumn("BidSize", bidSizeData);
      tableMaker.AddColumn("Ask", askData);
      tableMaker.AddColumn("AskSize", askSizeData);
      quotes = tableMaker.MakeTable(tm);
    }

    var result = trades.Raj(quotes, new[] { "Ticker", "Timestamp"});

    // Expected data
    {
      var tickerData = new[] { "AAPL", "AAPL", "AAPL", "IBM", "IBM"};
      var timestampData = new[] {
        DhDateTime.Parse("2021-04-05T09:10:00-0500"),
        DhDateTime.Parse("2021-04-05T09:31:00-0500"),
        DhDateTime.Parse("2021-04-05T16:00:00-0500"),
        DhDateTime.Parse("2021-04-05T16:00:00-0500"),
        DhDateTime.Parse("2021-04-05T16:30:00-0500")
      };

      var priceData = new[] { 2.5, 3.7, 3.0, 100.50, 110 };
      var sizeData = new[] { 52, 14, 73, 11, 6 };
      var bidData = new double?[] { 2.5, null, null, 97, 102 };
      var bidSizeData = new int?[] { 10, null, null, 5, 13 };
      var askData = new double?[] { 2.5, null, null, 105, 110 };
      var askSizeData = new int?[] { 83, null, null, 47, 15 };
      var tableComparer = new TableComparer();
      tableComparer.AddColumn("Ticker", tickerData);
      tableComparer.AddColumn("Timestamp", timestampData);
      tableComparer.AddColumn("Price", priceData);
      tableComparer.AddColumn("Size", sizeData);
      tableComparer.AddColumn("Bid", bidData);
      tableComparer.AddColumn("BidSize", bidSizeData);
      tableComparer.AddColumn("Ask", askData);
      tableComparer.AddColumn("AskSize", askSizeData);
      tableComparer.AssertEqualTo(result);
    }
  }
}
