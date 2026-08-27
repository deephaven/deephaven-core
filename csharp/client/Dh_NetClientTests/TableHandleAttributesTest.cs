//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class TableHandleAttributesTest {
  [Test]
  public async Task TableHandleAttributes() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;
    const Int64 numRows = 37;
    var t = thm.EmptyTable(numRows).Update("II = ii");
    await Assert.That(t.NumRows).IsEqualTo(numRows);
    await Assert.That(t.IsStatic).IsTrue();
  }

  [Test]
  public async Task TableHandleDynamicAttributes() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;
    var t = thm.TimeTable(1_000_000_000).Update("II = ii");
    await Assert.That(t.IsStatic).IsFalse();
  }

  [Test]
  public async Task TableHandleCreatedByDoPut() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;
    await Assert.That(table.IsStatic).IsTrue();
    // The columns all have the same size, so look at the source data for any one of them and get its size
    var expectedSize = ctx.ColumnData.ImportDate.Length;
    await Assert.That(table.NumRows).IsEqualTo(expectedSize);
  }
}
