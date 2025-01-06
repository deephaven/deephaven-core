using Deephaven.DeephavenClient;
using Xunit.Abstractions;

namespace Deephaven.DhClientTests;

public class InputTableTest {
  private readonly ITestOutputHelper _output;

  public InputTableTest(ITestOutputHelper output) {
    _output = output;
  }

  [Fact]
  public void TestInputTableAppend() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;

    var source = tm.EmptyTable(3).Update("A = ii", "B = ii + 100");
    // No keys, so InputTable will be in append-only mode.
    var inputTable = tm.InputTable(source);

    // expect inputTable to be {0, 100}, {1, 101}, {2, 102}
    {
      var aData = new Int64[] { 0, 1, 2 };
      var bData = new Int64[] { 100, 101, 102 };
      var tc = new TableComparer();
      tc.AddColumn("A", aData);
      tc.AddColumn("B", bData);
      tc.AssertEqualTo(inputTable);
    }

    var tableToAdd = tm.EmptyTable(2).Update("A = ii", "B = ii + 200");
    inputTable.AddTable(tableToAdd);

    // Because of append, expect input_table to be {0, 100}, {1, 101}, {2, 102}, {0, 200}, {1, 201}
    {
      var aData = new Int64[] { 0, 1, 2, 0, 1 };
      var bData = new Int64[] { 100, 101, 102, 200, 201 };
      var tc = new TableComparer();
      tc.AddColumn("A", aData);
      tc.AddColumn("B", bData);
      tc.AssertEqualTo(inputTable);
    }
  }


  [Fact]
  public void TestInputTableKeyed() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var tm = ctx.Client.Manager;

    var source = tm.EmptyTable(3).Update("A = ii", "B = ii + 100");
    // Keys = {"A"}, so InputTable will be in keyed mode
    var inputTable = tm.InputTable(source, "A");


    // expect input_table to be {0, 100}, {1, 101}, {2, 102}
    {
      var aData = new Int64[] { 0, 1, 2 };
      var bData = new Int64[] { 100, 101, 102 };
      var tc = new TableComparer();
      tc.AddColumn("A", aData);
      tc.AddColumn("B", bData);
      tc.AssertEqualTo(inputTable);
    }


    var tableToAdd = tm.EmptyTable(2).Update("A = ii", "B = ii + 200");
    inputTable.AddTable(tableToAdd);

    // Because key is "A", expect input_table to be {0, 200}, {1, 201}, {2, 102}
    {
      var aData = new Int64[] { 0, 1, 2 };
      var bData = new Int64[] { 200, 201, 102 };
      var tc = new TableComparer();
      tc.AddColumn("A", aData);
      tc.AddColumn("B", bData);
      tc.AssertEqualTo(inputTable);
    }
  }
}
