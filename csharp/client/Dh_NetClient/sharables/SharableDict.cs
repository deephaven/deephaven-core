//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using System.Collections;
using System.Diagnostics.CodeAnalysis;

namespace Deephaven.Dh_NetClient;

public class SharableDict<TValue> : IReadOnlyDictionary<Int64, TValue> {
  /// <summary>
  /// The singleton for an empty SharableDict&lt;TValue&gt;.
  /// </summary>
  public static readonly SharableDict<TValue> Empty = new();


  /// <summary>
  /// Makes the singleton for the empty SharableDict&lt;TValue&gt;.
  /// </summary>
  private SharableDict() {
    _root =
      ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<
        ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<
          ImmutableLeaf<TValue>>>>>>>>>>>.Empty; 
  }

  private readonly ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<
    ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<
      ImmutableLeaf<TValue>>>>>>>>>>> _root;

  public SharableDict(
    ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<
      ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<
        ImmutableLeaf<TValue>>>>>>>>>>> root) {
    _root = root;
  }

  public SharableDict<TValue> With(Int64 key, TValue value) {
    return new Destructured<TValue>(_root, key).RebuildWithNewLeafHere(value);
  }

  public SharableDict<TValue> Without(Int64 key) {
    var emptyDestructured = new Destructured<TValue>(Empty._root, 0);
    return new Destructured<TValue>(_root, key).RebuildWithoutLeafHere(in emptyDestructured);
  }

  public bool TryGetValue(Int64 key, [MaybeNullWhen(false)] out TValue value) {
    var s = new Destructured<TValue>(_root, key);
    if (!s.Depth10.TryGetChild(s.LeafIndex, out var temp)) {
      value = default;
      return false;
    }
    value = temp;
    return true;
  }

  public bool ContainsKey(Int64 key) {
    return TryGetValue(key, out _);
  }

  public (SharableDict<TValue>, SharableDict<TValue>, SharableDict<TValue>)
    CalcDifference(SharableDict<TValue> target) {
    var (added, removed, modified) = _root.CalcDifference(target._root);
    var aResult = new SharableDict<TValue>(added);
    var rResult = new SharableDict<TValue>(removed);
    var mResult = new SharableDict<TValue>(modified);
    return (aResult, rResult, mResult);
  }

  public TValue this[Int64 key] {
    get {
      if (!TryGetValue(key, out var value)) {
        throw new KeyNotFoundException();
      }

      return value;
    }
  }

  public IEnumerable<Int64> Keys => this.Select(kvp => kvp.Key);
  public IEnumerable<TValue> Values => this.Select(kvp => kvp.Value);

  public IEnumerator<KeyValuePair<Int64, TValue>> GetEnumerator() {
    // This could be written more nicely and recursively as a bunch of nested iterators
    // but the overhead of fetching each element would be pretty high, as each iterator
    // would call the MoveNext of the next iterator, etc.
    // Manually unrolling the structure into these nested loops is a little bit homely
    // but allows for more efficient code.
    var numChildren = _root.Children.Length;
    for (var i0 = 0; i0 != numChildren; ++i0) {
      var depth1 = _root.Children[i0];
      if (depth1.Count == 0) continue;
      for (var i1 = 0; i1 != numChildren; ++i1) {
        var depth2 = depth1.Children[i1];
        if (depth2.Count == 0) continue;
        for (var i2 = 0; i2 != numChildren; ++i2) {
          var depth3 = depth2.Children[i2];
          if (depth3.Count == 0) continue;
          for (var i3 = 0; i3 != numChildren; ++i3) {
            var depth4 = depth3.Children[i3];
            if (depth4.Count == 0) continue;
            for (var i4 = 0; i4 != numChildren; ++i4) {
              var depth5 = depth4.Children[i4];
              if (depth5.Count == 0) continue;
              for (var i5 = 0; i5 != numChildren; ++i5) {
                var depth6 = depth5.Children[i5];
                if (depth6.Count == 0) continue;
                for (var i6 = 0; i6 != numChildren; ++i6) {
                  var depth7 = depth6.Children[i6];
                  if (depth7.Count == 0) continue;
                  for (var i7 = 0; i7 != numChildren; ++i7) {
                    var depth8 = depth7.Children[i7];
                    if (depth8.Count == 0) continue;
                    for (var i8 = 0; i8 != numChildren; ++i8) {
                      var depth9 = depth8.Children[i8];
                      if (depth9.Count == 0) continue;
                      for (var i9 = 0; i9 != numChildren; ++i9) {
                        var depth10 = depth9.Children[i9];
                        foreach (var i10 in depth10.ValiditySet) {
                          var value = depth10.Children[i10];
                          var offset = Splitter.Merge(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10);
                          yield return KeyValuePair.Create(offset, value);
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

  public int Count => _root.Count;

  public override string ToString() {
    return string.Join(", ", this.Select(kvp => $"{kvp.Key}: {kvp.Value}"));
  }

  /// <summary>
  /// For unit tests
  /// </summary>
  internal ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableLeaf<TValue>>>>>>>>>>> RootForUnitTests
   => _root;

  internal int CountNodesForUnitTesting() {
    var set = new HashSet<object>(ReferenceEqualityComparer.Instance);
    _root.GatherNodesForUnitTesting(set);
    return set.Count;
  }
}
