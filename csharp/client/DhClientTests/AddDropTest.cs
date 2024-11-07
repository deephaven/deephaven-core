using Deephaven.DeephavenClient.Interop;
using Deephaven.DeephavenClient;
using Xunit.Abstractions;

namespace Deephaven.DhClientTests;

public class AddDropTest {
  private readonly ITestOutputHelper _output;

  public AddDropTest(ITestOutputHelper output) {
    _output = output;
  }

  [Fact]
  public void TestDropSomeColumns() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    var t = table.Update("II = ii").Where("Ticker == `AAPL`");
    var cn = ctx.ColumnNames;
    var t2 = t.DropColumns(cn.ImportDate, cn.Ticker, cn.Open, cn.Close);
    _output.WriteLine(t2.ToString(true));

    var volData = new Int64[] { 100000, 250000, 19000 };
    var iiData = new Int64[] { 5, 6, 7 };

    var tc = new TableComparer();
    tc.AddColumn("Volume", volData);
    tc.AddColumn("II", iiData);
    tc.AssertEqualTo(t2);
  }
}
