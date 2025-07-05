//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class HeadAndTailTest {
  [Fact]
  public void TestHeadAndTail() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var table = ctx.TestTable;

    table = table.Where("ImportDate == `2017-11-01`");

    var th = table.Head(2).Select("Ticker", "Volume");
    var tt = table.Tail(2).Select("Ticker", "Volume");

    {
      var expected = new TableMaker();

      expected.AddColumn("Ticker", ["XRX", "XRX"]);
      expected.AddColumn("Volume", [(Int64)345000, 87000]);
      TableComparer.AssertSame(expected, th);
    }

    {
      var expected = new TableMaker();

      expected.AddColumn("Ticker", ["ZNGA", "ZNGA"]);
      expected.AddColumn("Volume", [(Int64)46123, 48300]);
      TableComparer.AssertSame(expected, tt);
    }
  }
}
