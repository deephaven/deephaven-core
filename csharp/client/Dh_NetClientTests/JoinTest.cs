//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class JoinTest {
  [Fact]
  public void TestJoin() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var testTable = ctx.TestTable;

    using var table = testTable.Where("ImportDate == `2017-11-01`");
    using var lastClose = table.LastBy("Ticker");
    using var avgView = table.View("Ticker", "Volume").AvgBy("Ticker");

    using var joined = lastClose.NaturalJoin(avgView, ["Ticker"], ["ADV = Volume"]);
    using var filtered = joined.Select("Ticker", "Close", "ADV");

    var expected = new TableMaker();
    expected.AddColumn("Ticker", ["XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA"]);
    expected.AddColumn("Close", [53.8, 88.5, 38.7, 453, 26.7, 544.9]);
    expected.AddColumn("ADV", [216000, 6060842, 138000, 138000000, 123000, 47211.50]);

    TableComparer.AssertSame(expected, filtered);
  }

  [Fact]
  public void TestAj() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;

    TableHandle trades;
    {
      var tableMaker = new TableMaker();
      tableMaker.AddColumn("Ticker", ["AAPL", "AAPL", "AAPL", "IBM", "IBM"]);
      tableMaker.AddColumn("Timestamp", [
        DateTimeOffset.Parse("2021-04-05T09:10:00-0500"),
        DateTimeOffset.Parse("2021-04-05T09:31:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:00:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:00:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:30:00-0500")
      ]);
      tableMaker.AddColumn("Price", [2.5, 3.7, 3.0, 100.50, 110]);
      tableMaker.AddColumn("Size", [52, 14, 73, 11, 6]);
      trades = tableMaker.MakeTable(tm);
    }

    TableHandle quotes;
    {
      var tableMaker = new TableMaker();
      tableMaker.AddColumn("Ticker", ["AAPL", "AAPL", "IBM", "IBM", "IBM"]);
      tableMaker.AddColumn("Timestamp", [
        DateTimeOffset.Parse("2021-04-05T09:11:00-0500"),
        DateTimeOffset.Parse("2021-04-05T09:30:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:00:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:30:00-0500"),
        DateTimeOffset.Parse("2021-04-05T17:00:00-0500")
      ]);
      tableMaker.AddColumn("Bid", [2.5, 3.4, 97, 102, 108]);
      tableMaker.AddColumn("BidSize", [10, 20, 5, 13, 23]);
      tableMaker.AddColumn("Ask", [2.5, 3.4, 105, 110, 111]);
      tableMaker.AddColumn("AskSize", [83, 33, 47, 15, 5]);
      quotes = tableMaker.MakeTable(tm);
    }

    using var result = trades.Aj(quotes, ["Ticker", "Timestamp"]);

    // Expected data
    {
      var expected = new TableMaker();
      expected.AddColumn("Ticker", ["AAPL", "AAPL", "AAPL", "IBM", "IBM"]);
      expected.AddColumn("Timestamp", [
        DateTimeOffset.Parse("2021-04-05T09:10:00-0500"),
        DateTimeOffset.Parse("2021-04-05T09:31:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:00:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:00:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:30:00-0500")
      ]);
      expected.AddColumn("Price", [2.5, 3.7, 3.0, 100.50, 110]);
      expected.AddColumn("Size", [52, 14, 73, 11, 6]);
      expected.AddColumn("Bid", [(double?)null, 3.4, 3.4, 97, 102]);
      expected.AddColumn("BidSize", [(int?)null, 20, 20, 5, 13]);
      expected.AddColumn("Ask", [(double?)null, 3.4, 3.4, 105, 110]);
      expected.AddColumn("AskSize", [(int?)null, 33, 33, 47, 15]);
      TableComparer.AssertSame(expected, result);
    }
  }

  [Fact]
  public void TestRaj() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;

    TableHandle trades;
    {
      var tableMaker = new TableMaker();
      tableMaker.AddColumn("Ticker", ["AAPL", "AAPL", "AAPL", "IBM", "IBM"]);
      tableMaker.AddColumn("Timestamp", [
        DateTimeOffset.Parse("2021-04-05T09:10:00-0500"),
        DateTimeOffset.Parse("2021-04-05T09:31:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:00:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:00:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:30:00-0500")
      ]);
      tableMaker.AddColumn("Price", [2.5, 3.7, 3.0, 100.50, 110]);
      tableMaker.AddColumn("Size", [52, 14, 73, 11, 6]);
      trades = tableMaker.MakeTable(tm);
    }

    TableHandle quotes;
    {
      var tableMaker = new TableMaker();
      tableMaker.AddColumn("Ticker", ["AAPL", "AAPL", "IBM", "IBM", "IBM"]);
      tableMaker.AddColumn("Timestamp", [
        DateTimeOffset.Parse("2021-04-05T09:11:00-0500"),
        DateTimeOffset.Parse("2021-04-05T09:30:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:00:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:30:00-0500"),
        DateTimeOffset.Parse("2021-04-05T17:00:00-0500")
      ]);
      tableMaker.AddColumn("Bid", [2.5, 3.4, 97, 102, 108]);
      tableMaker.AddColumn("BidSize", [10, 20, 5, 13, 23]);
      tableMaker.AddColumn("Ask", [2.5, 3.4, 105, 110, 111]);
      tableMaker.AddColumn("AskSize", [83, 33, 47, 15, 5]);
      quotes = tableMaker.MakeTable(tm);
    }

    var result = trades.Raj(quotes, new[] { "Ticker", "Timestamp"});

    // Expected data
    {
      var expected = new TableMaker();
      expected.AddColumn("Ticker", ["AAPL", "AAPL", "AAPL", "IBM", "IBM"]);
      expected.AddColumn("Timestamp", [
        DateTimeOffset.Parse("2021-04-05T09:10:00-0500"),
        DateTimeOffset.Parse("2021-04-05T09:31:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:00:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:00:00-0500"),
        DateTimeOffset.Parse("2021-04-05T16:30:00-0500")
      ]);
      expected.AddColumn("Price", [2.5, 3.7, 3.0, 100.50, 110]);
      expected.AddColumn("Size", [52, 14, 73, 11, 6]);
      expected.AddColumn("Bid", [(double?)2.5, null, null, 97, 102]);
      expected.AddColumn("BidSize", [(int?)10, null, null, 5, 13]);
      expected.AddColumn("Ask", [(double?)2.5, null, null, 105, 110]);
      expected.AddColumn("AskSize", [(int?)83, null, null, 47, 15]);

      TableComparer.AssertSame(expected, result);
    }
  }
}
