using Deephaven.DeephavenClient;
using Xunit.Abstractions;

namespace Deephaven.DhClientTests;

public class TableHandleAttributesTest {
  private readonly ITestOutputHelper _output;

  public TableHandleAttributesTest(ITestOutputHelper output) {
    _output = output;
  }

  [Fact]
  public void TableHandleAttributes() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;
    const Int64 numRows = 37;
    var t = thm.EmptyTable(numRows).Update("II = ii");
    Assert.Equal(numRows, t.NumRows);
    Assert.True(t.IsStatic);
  }

  [Fact]
  public void TableHandleDynamicAttributes() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;
    var t = thm.TimeTable(1_000_000_000).Update("II = ii");
    Assert.False(t.IsStatic);
  }

  [Fact]
  public void TableHandleCreatedByDoPut() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;
    Assert.True(table.IsStatic);
    // The columns all have the same size, so look at the source data for any one of them and get its size
    var expectedSize = ctx.ColumnData.ImportDate.Length;
    Assert.Equal(expectedSize, table.NumRows);
  }
}
