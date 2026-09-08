//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

using System.Collections;
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class ReadOnlyListAdaptersTest {
  [Test]
  public async Task AsIList() {
    IList list = MakeReadOnlyListAdapterForValueTypes();
    await Assert.That(list.Count).IsEqualTo(4);
    await Assert.That(list.IsFixedSize).IsTrue();
    await Assert.That(list.IsReadOnly).IsTrue();
    object?[] expected = [1, 2, null, 4];
    await Assert.That(list).IsEquivalentTo(expected);
    await Assert.That(list[0]).IsEqualTo(1);
    await Assert.That(list.Contains(2)).IsTrue();
    await Assert.That(list.Contains(null)).IsTrue();
    await Assert.That(list.Contains(3)).IsFalse();
    var dest = new object?[5];
    list.CopyTo(dest, 1);
    await Assert.That(dest).IsEquivalentTo(new object?[] { null, 1, 2, null, 4 });

    // test enumeration
    var copy = list.Cast<object>().ToArray();
    await Assert.That(list).IsEquivalentTo(copy);

    await Assert.That(() => list.Clear()).Throws<NotImplementedException>();
    await Assert.That(() => list.Add(5)).Throws<NotImplementedException>();
    await Assert.That(() => list.Remove(1)).Throws<NotImplementedException>();
    await Assert.That(() => list.Insert(0, 1)).Throws<NotImplementedException>();
    await Assert.That(() => list.RemoveAt(0)).Throws<NotImplementedException>();
  }

  [Test]
  public async Task AsIListOfT() {
    IList<Int32> list = MakeReadOnlyListAdapterForValueTypes();
    await Assert.That(list.Count).IsEqualTo(4);
    await Assert.That(list.IsReadOnly).IsTrue();
    Int32[] expected = [1, 2, DeephavenConstants.NullInt, 4];
    await Assert.That(list).IsEquivalentTo(expected.ToArray());
    await Assert.That(list[0]).IsEqualTo(1);
    await Assert.That(list.Contains(2)).IsTrue();
    await Assert.That(list.Contains(3)).IsFalse();
    var dest = new int[5];
    list.CopyTo(dest, 1);
    await Assert.That(dest).IsEquivalentTo(new Int32[] { 0, 1, 2, DeephavenConstants.NullInt, 4 });

    // test enumeration
    var copy = list.Select(x => x).ToArray();
    await Assert.That(list).IsEquivalentTo(copy);

    await Assert.That(() => list.Clear()).Throws<NotImplementedException>();
    await Assert.That(() => list.Add(5)).Throws<NotImplementedException>();
    await Assert.That(() => list.Remove(1)).Throws<NotImplementedException>();
    await Assert.That(() => list.Insert(0, 1)).Throws<NotImplementedException>();
    await Assert.That(() => list.RemoveAt(0)).Throws<NotImplementedException>();
  }

  [Test]
  public async Task AsIListOfNullableT() {
    IList<Int32?> list = MakeReadOnlyListAdapterForValueTypes();
    await Assert.That(list.Count).IsEqualTo(4);
    await Assert.That(list.IsReadOnly).IsTrue();
    Int32?[] expected = [1, 2, null, 4];
    await Assert.That(list).IsEquivalentTo(expected);
    await Assert.That(list[0]).IsEqualTo(1);
    await Assert.That(list.Contains(2)).IsTrue();
    await Assert.That(list.Contains(3)).IsFalse();
    var dest = new Int32?[5];
    list.CopyTo(dest, 1);
    await Assert.That(dest).IsEquivalentTo(new Int32?[] { null, 1, 2, null, 4 });

    // test enumeration
    var copy = list.Select(x => x).ToArray();
    await Assert.That(list).IsEquivalentTo(copy);

    await Assert.That(() => list.Clear()).Throws<NotImplementedException>();
    await Assert.That(() => list.Add(5)).Throws<NotImplementedException>();
    await Assert.That(() => list.Remove(1)).Throws<NotImplementedException>();
    await Assert.That(() => list.Insert(0, 1)).Throws<NotImplementedException>();
    await Assert.That(() => list.RemoveAt(0)).Throws<NotImplementedException>();
  }

  private static ReadOnlyListAdapterForValueTypes<Int32> MakeReadOnlyListAdapterForValueTypes() {
    var int32Data = new Int32?[] { 1, 2, null, 4 };
    var adapter = new ReadOnlyListAdapterForValueTypes<Int32>(int32Data, DeephavenConstants.NullInt);
    return adapter;
  }
}
