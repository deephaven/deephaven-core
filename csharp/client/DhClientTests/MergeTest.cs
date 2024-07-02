using Deephaven.DeephavenClient;

namespace Deephaven.DhClientTests;

public class MergeTest {
  [Fact]
  public void TestMerge() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var testTable = ctx.TestTable;

    using var table = testTable.Where("ImportDate == `2017-11-01`");

    // Run a merge by fetching two tables and them merging them
    var aaplTable = table.Where("Ticker == `AAPL`").Tail(10);
    var zngaTable = table.Where("Ticker == `ZNGA`").Tail(10);

    var merged = aaplTable.Merge(new[] { zngaTable} );

    var importDateData = new[] {
      "2017-11-01", "2017-11-01", "2017-11-01",
      "2017-11-01", "2017-11-01"};
    var tickerData = new[] { "AAPL", "AAPL", "AAPL", "ZNGA", "ZNGA"};
    var openData = new[] { 22.1, 26.8, 31.5, 541.2, 685.3 };
    var closeData = new[] { 23.5, 24.2, 26.7, 538.2, 544.9 };
    var volData = new Int64[] { 100000, 250000, 19000, 46123, 48300 };

    var tc = new TableComparer();
    tc.AddColumn("ImportDate", importDateData);
    tc.AddColumn("Ticker", tickerData);
    tc.AddColumn("Open", openData);
    tc.AddColumn("Close", closeData);
    tc.AddColumn("Volume", volData);
    tc.AssertEqualTo(merged);
  }
}