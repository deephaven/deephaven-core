//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//

using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class SharableDictTest {
  [Test]
  public async Task Simple() {
    var d = SharableDict<string>.Empty;
    var dict = d.With(10, "hello")
      .With(11, "world")
      .With(1000, "Deephaven");

    await Assert.That(ContainsEntry(dict, 10, "hello")).IsTrue();
    await Assert.That(ContainsEntry(dict, 11, "world")).IsTrue();
    await Assert.That(ContainsEntry(dict, 1000, "Deephaven")).IsTrue();
    await Assert.That(dict.TryGetValue(1001, out _)).IsFalse();
    await Assert.That(dict.Count).IsEqualTo(3);

    var dict2 = dict.With(11, "world v2")
      .With(1000, "Deephaven v2");

    // dict2 has some new values
    await Assert.That(ContainsEntry(dict2, 10, "hello")).IsTrue();
    await Assert.That(ContainsEntry(dict2, 11, "world v2")).IsTrue();
    await Assert.That(ContainsEntry(dict2, 1000, "Deephaven v2")).IsTrue();
    await Assert.That(dict2.Count).IsEqualTo(3);

    // Initial dict unchanged
    await Assert.That(ContainsEntry(dict, 10, "hello")).IsTrue();
    await Assert.That(ContainsEntry(dict, 11, "world")).IsTrue();
    await Assert.That(ContainsEntry(dict, 1000, "Deephaven")).IsTrue();
    await Assert.That(dict.Count).IsEqualTo(3);
  }

  [Test]
  public async Task Ordering() {
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

    await Assert.That(list.SequenceEqual(expected)).IsTrue();
  }

  [Test]
  public async Task Canonicalizes() {
    var d0 = SharableDict<string>.Empty;
    var d1 = d0.With(1_000, "hello");
    var d2 = d1.With(1_000_000, "there");
    var d3 = d2.With(1_000_000_000, "Deephaven");

    await Assert.That(d0.Count).IsEqualTo(0);
    await Assert.That(d1.Count).IsEqualTo(1);
    await Assert.That(d2.Count).IsEqualTo(2);
    await Assert.That(d3.Count).IsEqualTo(3);

    var newd2 = d3.Without(1_000);
    var newd1 = newd2.Without(1_000_000);
    var newd0 = newd1.Without(1_000_000_000);

    await Assert.That(ReferenceEquals(newd0.RootForUnitTests, d0.RootForUnitTests)).IsTrue();
  }

  [Test]
  public async Task NonexistentRemoveIsNoop() {
    var dict = SharableDict<string>.Empty
      .With(10, "hello")
      .With(11, "world")
      .With(1000, "Deephaven");

    var d2 = dict.Without(999); // nonexistent key

    await Assert.That(ReferenceEquals(dict, d2)).IsTrue();
  }

  [Test]
  public async Task Iterates() {
    var dict = SharableDict<string>.Empty;
    for (var i = 0; i != 10000; ++i) {
      dict = dict.With(i * 37, "hello" + i);
    }

    var nextIndex = 0;
    foreach (var (k, v) in dict) {
      await Assert.That(k).IsEqualTo(nextIndex * 37);
      await Assert.That(v).IsEqualTo("hello" + nextIndex);
      ++nextIndex;
    }
  }

  [Test]
  public async Task Difference() {
    var dict1 = SharableDict<int>.Empty;
    for (var i = 0; i != 10; ++i) {
      dict1 = dict1.With(i, i * 37);
    }

    var dict2 = dict1
      .With(100, 999) // add
      .With(1000, 9999) // add
      .Without(3) // remove
      .Without(5) // remove
      .With(7, 12345) // modify
      .With(-1, 999); // add

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

    await Assert.That(a).IsEquivalentTo(aExpected);
    await Assert.That(r).IsEquivalentTo(rExpected);
    await Assert.That(m).IsEquivalentTo(mExpected);

    await Assert.That(a.CountNodesForUnitTesting()).IsEqualTo(32);
    await Assert.That(r.CountNodesForUnitTesting()).IsEqualTo(21);
    await Assert.That(m.CountNodesForUnitTesting()).IsEqualTo(21);
  }

  [Test]
  public async Task DictIsEfficientForLargeDenseSets() {
    // These should asymptote towards 64 elements per node.

    // An empty dict costs 11 nodes
    await TestDenseEfficiency(0, 11);

    // A dict densely packed with the first 64 integers costs 21 nodes
    // Efficency: 21 nodes per 64 elements
    // 0.328 nodes per element, 3.048 elements per node
    await TestDenseEfficiency(64, 21);

    // A dict densely packed with the first 4096 integers costs 84 nodes
    // Efficency: 84 nodes per 4096 elements
    // 0.021 nodes per element, 48.76 elements per node
    await TestDenseEfficiency(4096, 84);

    // A dict densely packed with the first 65536 integers costs 1059 nodes
    // Efficency: 84 nodes per 4096 elements
    // 0.016 nodes per element, 61.88 elements per node
    await TestDenseEfficiency(65536, 1059);
  }

  private static async Task TestDenseEfficiency(int count, int expectedNodeCount) {
    var dict = SharableDict<int>.Empty;
    for (var i = 0; i != count; ++i) {
      dict = dict.With(i, i * 1111);
    }
    for (var i = 0; i != count; ++i) {
      await Assert.That(dict.TryGetValue(i, out var value)).IsTrue();
      await Assert.That(value).IsEqualTo(i * 1111);
    }

    await Assert.That(dict.Count).IsEqualTo(count);
    await Assert.That(dict.CountNodesForUnitTesting()).IsEqualTo(expectedNodeCount);
  }

  private static bool ContainsEntry<T>(SharableDict<T> dict, Int64 key, T expected) {
    return dict.TryGetValue(key, out var value) && Object.Equals(value, expected);
  }
}
