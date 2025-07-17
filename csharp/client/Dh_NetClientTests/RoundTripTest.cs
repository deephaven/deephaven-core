using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class RoundTripTest {
  [Fact]
  public void Elements() {
    var tm = new TableMaker();
    tm.AddColumn("Col1", [0, 1, 2, 3, 4, 5]);
    tm.AddColumn("Col2", ["a", "b", "c", "d", "e", "f"]);

    var at = tm.ToArrowTable();
    var ct = ArrowUtil.ToClientTable(at);
    var at2 = ct.ToArrowTable();
    TableComparer.AssertSame(at, at2);
  }

  [Fact]
  public void Nested() {
    var tm = new TableMaker();
    tm.AddColumn<int[]>("Col1", [[0, 1, 2], [3, 4], [5]]);

    var at = tm.ToArrowTable();

    // TODO(kosak): When you fix this, update DH-19910
    var ex = Assert.Throws<Exception>(() => {
      var ct = ArrowUtil.ToClientTable(at);
      var at2 = ct.ToArrowTable();
      TableComparer.AssertSame(at, at2);
    });

    Assert.Contains("Arrow type list is not supported", ex.Message);
  }
}
