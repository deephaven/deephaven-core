//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//

using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class SharableDictTest {
  [Fact]
  public void Simple() {
    var d = SharableDict<string>.Empty;
    var dict = d.With(10, "hello")
      .With(11, "world")
      .With(1000, "Deephaven");

    Assert.True(ContainsEntry(dict, 10, "hello"));
    Assert.True(ContainsEntry(dict, 11, "world"));
    Assert.True(ContainsEntry(dict, 1000, "Deephaven"));
    Assert.False(dict.TryGetValue(1001, out _));
    Assert.Equal(3, dict.Count);

    var dict2 = dict.With(11, "world v2")
      .With(1000, "Deephaven v2");

    // dict2 has some new values
    Assert.True(ContainsEntry(dict2, 10, "hello"));
    Assert.True(ContainsEntry(dict2, 11, "world v2"));
    Assert.True(ContainsEntry(dict2, 1000, "Deephaven v2"));
    Assert.Equal(3, dict2.Count);

    // Initial dict unchanged
    Assert.True(ContainsEntry(dict, 10, "hello"));
    Assert.True(ContainsEntry(dict, 11, "world"));
    Assert.True(ContainsEntry(dict, 1000, "Deephaven"));
    Assert.Equal(3, dict.Count);
  }

  [Fact]
  public void Ordering() {
    var dict = SharableDict<int>.Empty;
    dict = dict.With(3, 1000)
      .With(10, 2000)
      .With(-20, 3000)
      .With(-10, 4000);

    var list = dict.ToList();
    var expected = new List<KeyValuePair<Int64, int>> {
      new(-20, 3000),
      new(-10, 4000),
      new(3, 1000),
      new(10, 2000)
    };

    Assert.Equal(expected, list);
  }

  [Fact]
  public void Canonicalizes() {
    var d0 = SharableDict<string>.Empty;
    var d1 = d0.With(1_000, "hello");
    var d2 = d1.With(1_000_000, "there");
    var d3 = d2.With(1_000_000_000, "Deephaven");

    Assert.Equal(0, d0.Count);
    Assert.Equal(1, d1.Count);
    Assert.Equal(2, d2.Count);
    Assert.Equal(3, d3.Count);

    var newd2 = d3.Without(1_000);
    var newd1 = newd2.Without(1_000_000);
    var newd0 = newd1.Without(1_000_000_000);

    Assert.True(ReferenceEquals(newd0.RootForUnitTests, d0.RootForUnitTests));
  }

  [Fact]
  public void Iterates() {
    var dict = SharableDict<string>.Empty;
    for (var i = 0; i != 10000; ++i) {
      dict = dict.With(i * 37, "hello" + i);
    }

    var nextIndex = 0;
    foreach (var (k, v) in dict) {
      Assert.Equal(nextIndex * 37, k);
      Assert.Equal("hello" + nextIndex, v);
      ++nextIndex;
    }
  }

  [Fact]
  public void Difference() {
    var dict1 = SharableDict<int>.Empty;
    for (var i = 0; i != 10; ++i) {
      dict1 = dict1.With(i, i * 37);
    }

    var dict2 = dict1
      .With(100, 999)  // add
      .With(1000, 9999)  // add
      .Without(3)  // remove
      .Without(5)  // remove
      .With(7, 12345)  // modify
      .With(-1, 999);  // add

    var (a, r, m) = dict1.CalcDifference(dict2);

    var aExpected = new List<KeyValuePair<Int64, int>> {
      new(-1, 999),
      new(100, 999),
      new(1000, 9999)
    };

    var rExpected = new List<KeyValuePair<Int64, int>> {
      new(3, 3 * 37),
      new(5, 5 * 37)
    };

    var mExpected = new List<KeyValuePair<Int64, int>> {
      new(7, 12345)
    };

    Assert.Equal(aExpected, a);
    Assert.Equal(rExpected, r);
    Assert.Equal(mExpected, m);

    Assert.Equal(32, a.CountNodesForUnitTesting());
    Assert.Equal(21, r.CountNodesForUnitTesting());
    Assert.Equal(21, m.CountNodesForUnitTesting());
  }

  [Fact]
  public void DictIsEfficientForLargeDenseSets() {
    // These should asymptote towards 64 elements per node.

    // An empty dict costs 11 nodes
    TestDenseEfficiency(0, 11);

    // A dict densely packed with the first 64 integers costs 21 nodes
    // Efficency: 21 nodes per 64 elements
    // 0.328 nodes per element, 3.048 elements per node
    TestDenseEfficiency(64, 21);

    // A dict densely packed with the first 4096 integers costs 84 nodes
    // Efficency: 84 nodes per 4096 elements
    // 0.021 nodes per element, 48.76 elements per node
    TestDenseEfficiency(4096, 84);

    // A dict densely packed with the first 65536 integers costs 1059 nodes
    // Efficency: 84 nodes per 4096 elements
    // 0.016 nodes per element, 61.88 elements per node
    TestDenseEfficiency(65536, 1059);
  }

  private static void TestDenseEfficiency(int count, int expectedNodeCount) {
    var dict = SharableDict<int>.Empty;
    for (var i = 0; i != count; ++i) {
      dict = dict.With(i, i * 1111);
    }
    for (var i = 0; i != count; ++i) {
      Assert.True(dict.TryGetValue(i, out var value));
      Assert.Equal(i * 1111, value);
    }

    Assert.Equal(count, dict.Count);
    Assert.Equal(expectedNodeCount, dict.CountNodesForUnitTesting());
  }

  private static bool ContainsEntry<T>(SharableDict<T> dict, Int64 key, T expected) {
    return dict.TryGetValue(key, out var value) && Object.Equals(value, expected);
  }
}
