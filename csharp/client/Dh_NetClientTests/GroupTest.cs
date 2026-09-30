//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class GroupTest {
  [Test]
  public async Task GroupATable() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var maker = new TableMaker();
    maker.AddColumn("Type", [
      "Granny Smith",
      "Granny Smith",
      "Gala",
      "Gala",
      "Golden Delicious",
      "Golden Delicious"
    ]);
    maker.AddColumn("Color", [
      "Green", "Green", "Red-Green", "Orange-Green", "Yellow", "Yellow"
    ]);
    maker.AddColumn("Weight", [
      102, 85, 79, 92, 78, 99
    ]);
    maker.AddColumn("Calories", [
      53, 48, 51, 61, 46, 57
    ]);
    using var t1 = maker.MakeTable(ctx.Client.Manager);

    using var grouped = t1.By("Type");
    Console.WriteLine(grouped.ToString(true, true));

    var expected = new TableMaker();
    expected.AddColumn("Type", ["Granny Smith", "Gala", "Golden Delicious"]);
    expected.AddColumn<List<string>>("Color",
      [["Green", "Green"], ["Red-Green", "Orange-Green"], ["Yellow", "Yellow"]]);
    expected.AddColumn<List<Int32>>("Weight", [[102, 85], [79, 92], [78, 99]]);
    expected.AddColumn<List<Int32>>("Calories", [[53, 48], [51, 61], [46, 57]]);
    await Assert.That(() => TableComparer.AssertSame(expected, grouped)).ThrowsNothing();
  }

  [Test]
  public async Task NestedListsNotSupported() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var maker = new TableMaker();
    maker.AddColumn<List<List<Int32>>>("Value", [
      [[1, 2, 3]],
      [[4, 5]]
    ]);
    using var t = maker.MakeTable(ctx.Client.Manager);
    await Assert.That(() => t.ToClientTable()).Throws<Exception>();
  }
}