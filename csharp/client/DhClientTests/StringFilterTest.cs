using Deephaven.DeephavenClient;

namespace Deephaven.DhClientTests;

public class StringFilterTest {
  [Fact]
  public void StringFilter() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var testTable = ctx.TestTable;

    var t2 = testTable.Where("ImportDate == `2017-11-01`").Select("Ticker", "Close");

    {
      var tickerData = new[] { "AAPL", "AAPL", "AAPL", "ZNGA", "ZNGA" };
      var closeData = new[] { 23.5, 24.2, 26.7, 538.2, 544.9 };
      TestFilter("Contains A", t2.Where("Ticker.contains(`A`)"),
        tickerData, closeData);
    }

    {
      var tickerData = new string[] { };
      var closeData = new double[] { };
      TestFilter("Starts with BL", t2.Where("Ticker.startsWith(`BL`)"),
        tickerData, closeData);
    }

    {
      var tickerData = new[] { "XRX", "XRX"};
      var closeData = new[] { 88.2, 53.8 };
      TestFilter("Ends with X", t2.Where("Ticker.endsWith(`X`)"),
        tickerData, closeData);
    }

    {
      var tickerData = new[] { "IBM" };
      var closeData = new[] { 38.7 };
      TestFilter("Matches ^I.*M$", t2.Where("Ticker.matches(`^I.*M$`)"),
        tickerData, closeData);
    }
  }

  private static void TestFilter(string description, TableHandle filteredTable,
    string[] tickerData, double[] closeData) {
    var tc = new TableComparer();
    tc.AddColumn("Ticker", tickerData);
    tc.AddColumn("Close", closeData);
    tc.AssertEqualTo(filteredTable);
  }
}
