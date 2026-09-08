//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class InputTableTest {
  [Test]
  public async Task TestInputTableAppend() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;

    var source = tm.EmptyTable(3).Update("A = ii", "B = ii + 100");
    // No keys, so InputTable will be in append-only mode.
    var inputTable = tm.InputTable(source);

    // expect inputTable to be {0, 100}, {1, 101}, {2, 102}
    {
      var expected = new TableMaker();
      expected.AddColumn("A", [(Int64)0, 1, 2]);
      expected.AddColumn("B", [(Int64)100, 101, 102]);
      await Assert.That(() => TableComparer.AssertSame(expected, inputTable)).ThrowsNothing();
    }

    var tableToAdd = tm.EmptyTable(2).Update("A = ii", "B = ii + 200");
    inputTable.AddTable(tableToAdd);

    // Because of append, expect input_table to be {0, 100}, {1, 101}, {2, 102}, {0, 200}, {1, 201}
    {
      var aData = new Int64[] { 0, 1, 2, 0, 1 };
      var bData = new Int64[] { 100, 101, 102, 200, 201 };
      var expected = new TableMaker();
      expected.AddColumn("A", aData);
      expected.AddColumn("B", bData);
      await Assert.That(() => TableComparer.AssertSame(expected, inputTable)).ThrowsNothing();
    }
  }


  [Test]
  public async Task TestInputTableKeyed() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;

    var source = tm.EmptyTable(3).Update("A = ii", "B = ii + 100");
    // Keys = {"A"}, so InputTable will be in keyed mode
    var inputTable = tm.InputTable(source, "A");


    // expect inputTable to be {0, 100}, {1, 101}, {2, 102}
    {
      var aData = new Int64[] { 0, 1, 2 };
      var bData = new Int64[] { 100, 101, 102 };
      var expected = new TableMaker();
      expected.AddColumn("A", aData);
      expected.AddColumn("B", bData);
      await Assert.That(() => TableComparer.AssertSame(expected, inputTable)).ThrowsNothing();
    }

    var tableToAdd = tm.EmptyTable(2).Update("A = ii", "B = ii + 200");
    inputTable.AddTable(tableToAdd);

    // Because key is "A", expect inputTable to be {0, 200}, {1, 201}, {2, 102}
    {
      var aData = new Int64[] { 0, 1, 2 };
      var bData = new Int64[] { 200, 201, 102 };
      var expected = new TableMaker();
      expected.AddColumn("A", aData);
      expected.AddColumn("B", bData);
      await Assert.That(() => TableComparer.AssertSame(expected, inputTable)).ThrowsNothing();
    }
  }
}
