//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class StringFilterTest {
  [Fact]
  public void StringFilter() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var testTable = ctx.TestTable;

    var t2 = testTable.Where("ImportDate == `2017-11-01`").Select("Ticker", "Close");

    {
      TestFilter("Contains A", t2.Where("Ticker.contains(`A`)"),
        ["AAPL", "AAPL", "AAPL", "ZNGA", "ZNGA"],
        [23.5, 24.2, 26.7, 538.2, 544.9]);
    }

    {
      TestFilter("Starts with BL", t2.Where("Ticker.startsWith(`BL`)"),
        [], []);
    }

    {
      TestFilter("Ends with X", t2.Where("Ticker.endsWith(`X`)"),
        ["XRX", "XRX"], [88.2, 53.8]);
    }

    {
      TestFilter("Matches ^I.*M$", t2.Where("Ticker.matches(`^I.*M$`)"),
        ["IBM"], [38.7]);
    }
  }

  private static void TestFilter(string description, TableHandle filteredTable,
    string[] tickerData, double[] closeData) {
    var expected = new TableMaker();
    expected.AddColumn("Ticker", tickerData);
    expected.AddColumn("Close", closeData);

    try {
      TableComparer.AssertSame(expected, filteredTable);
    } catch (Exception e) {
      throw new AggregateException($"While processing {description}", e);
    }
  }
}
