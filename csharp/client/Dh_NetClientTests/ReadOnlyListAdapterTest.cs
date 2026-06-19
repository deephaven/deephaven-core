//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

using System.Collections;
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class ReadOnlyListAdaptersTest {
  [Fact]
  public void AsIList() {
    IList list = MakeReadOnlyListAdapterForValueTypes();
    Assert.Equal(4, list.Count);
    Assert.True(list.IsFixedSize);
    Assert.True(list.IsReadOnly);
    object?[] expected = [1, 2, null, 4];
    Assert.Equivalent(expected, list);
    Assert.Equal(1, list[0]);
    Assert.True(list.Contains(2));
    Assert.True(list.Contains(null));
    Assert.False(list.Contains(3));
    var dest = new object?[5];
    list.CopyTo(dest, 1);
    Assert.Equivalent(new object?[]{null, 1, 2, null, 4}, dest);

    // test enumeration
    var copy = list.Cast<object>().ToArray();
    Assert.Equivalent(copy, list);

    Assert.Throws<NotImplementedException>(() => list.Clear());
    Assert.Throws<NotImplementedException>(() => list.Add(5));
    Assert.Throws<NotImplementedException>(() => list.Remove(1));
    Assert.Throws<NotImplementedException>(() => list.Insert(0, 1));
    Assert.Throws<NotImplementedException>(() => list.RemoveAt(0));
  }

  [Fact]
  public void AsIListOfT() {
    IList<Int32> list = MakeReadOnlyListAdapterForValueTypes();
    Assert.Equal(4, list.Count);
    Assert.True(list.IsReadOnly);
    Int32[] expected = [1, 2, DeephavenConstants.NullInt, 4];
    // Comparing expected and list directly doesn't work because of something
    // in the way XUnit works. Probably because my Enumerable, Enumerable<int>
    // and Enumerable<int?> all give different answers. To be investigated.
    var listAsArray = list.ToArray();
    Assert.Equal(expected.ToArray(), listAsArray);
    Assert.Equal(1, list[0]);
    Assert.True(list.Contains(2));
    Assert.False(list.Contains(3));
    var dest = new int[5];
    list.CopyTo(dest, 1);
    Assert.Equal(new Int32[] { 0, 1, 2, DeephavenConstants.NullInt, 4 }, dest);

    // test enumeration
    var copy = list.Select(x => x).ToArray();
    Assert.Equal(copy, listAsArray);

    Assert.Throws<NotImplementedException>(() => list.Clear());
    Assert.Throws<NotImplementedException>(() => list.Add(5));
    Assert.Throws<NotImplementedException>(() => list.Remove(1));
    Assert.Throws<NotImplementedException>(() => list.Insert(0, 1));
    Assert.Throws<NotImplementedException>(() => list.RemoveAt(0));
  }

  [Fact]
  public void AsIListOfNullableT() {
    IList<Int32?> list = MakeReadOnlyListAdapterForValueTypes();
    Assert.Equal(4, list.Count);
    Assert.True(list.IsReadOnly);
    Int32?[] expected = [1, 2, null, 4];
    // Comparing expected and list directly doesn't work because of something
    // in the way XUnit works. Probably because my Enumerable, Enumerable<int>
    // and Enumerable<int?> all give different answers. To be investigated.
    var listAsArray = list.ToArray();
    Assert.Equal(expected, listAsArray);
    Assert.Equal(1, list[0]);
    Assert.True(list.Contains(2));
    Assert.False(list.Contains(3));
    var dest = new Int32?[5];
    list.CopyTo(dest, 1);
    Assert.Equal(new Int32?[] { null, 1, 2, null, 4 }, dest);

    // test enumeration
    var copy = list.Select(x => x).ToArray();
    Assert.Equal(copy, listAsArray);

    Assert.Throws<NotImplementedException>(() => list.Clear());
    Assert.Throws<NotImplementedException>(() => list.Add(5));
    Assert.Throws<NotImplementedException>(() => list.Remove(1));
    Assert.Throws<NotImplementedException>(() => list.Insert(0, 1));
    Assert.Throws<NotImplementedException>(() => list.RemoveAt(0));
  }

  private static ReadOnlyListAdapterForValueTypes<Int32> MakeReadOnlyListAdapterForValueTypes() {
    var int32Data = new Int32?[] { 1, 2, null, 4 };
    var adapter = new ReadOnlyListAdapterForValueTypes<Int32>(int32Data, DeephavenConstants.NullInt);
    return adapter;
  }
}
