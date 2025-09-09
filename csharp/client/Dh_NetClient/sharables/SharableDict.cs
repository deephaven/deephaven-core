//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using System.Collections;
using System.Diagnostics.CodeAnalysis;

namespace Deephaven.Dh_NetClient;

/// <summary>
/// This class implements an immutable dictionary data structure with Int64 keys and values of user-specified type.
/// The dictionary is implemented as a fixed-depth tree (the internal nodes have arity 64, and the tree has depth 11).
/// Looking up an entry by key is constant time, because it traverses a fixed 11 nodes from root to leaf.
/// Looking up an entry by rank (not implemented yet) is also constant time, but the constant is larger,
/// because it needs to traverse 11 nodes but also scan children counts at every node.
/// On the other hand, simple scans like this are fast on modern CPUs.
/// Modifications the tree are performed by making a new tree that shares most of its substructure with the old tree,
/// differing only on the path from root to new value. Callers are welcome to keep multiple versions of the dictionary
/// if their application needs to. There are no locks and no caller synchronization is required.
///
/// The major operations are called "With" and "Without". The With(k, v) method returns a new dictionary
/// which is just like the current one except it also has (new or modified) entry mapping "k" to "v". The
/// Without(key) method returns a new dictionary just like the current one except it lacks an entry for "k".
/// In the future we will provide batch versions of With and Without for greater efficiency.
///
/// There is also a "CalcDifference" operations which compares the current dictionary with another and
/// returns three dictionaries representing the difference.
///
/// The data structure is also enumerable. As with any immutable data structure, there is no concern about
/// enumerating while concurrently modifying (because there is no "modify" operation per se).
/// </summary>
/// <typeparam name="TValue"></typeparam>
public class SharableDict<TValue> : IReadOnlyDictionary<Int64, TValue> {
  /// <summary>
  /// The singleton for an empty SharableDict&lt;TValue&gt;. This is how you get your initial SharableDict.
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

  /// <summary>
  /// Constructor. Used internally.
  /// </summary>
  /// <param name="root"></param>
  internal SharableDict(
    ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<
      ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<
        ImmutableLeaf<TValue>>>>>>>>>>> root) {
    _root = root;
  }

  /// <summary>
  /// Makes a new SharableDict with the same entries as 'this', except that also 'key' now maps to 'value'.
  /// Logically, this will be an add operation (if this dictionary does not currently contain 'key') or a modify operation
  /// (if it does). In all cases 'this' dictionary is unchanged.
  /// </summary>
  /// <param name="key">The new key</param>
  /// <param name="value">The new value</param>
  /// <returns>The new dictionary</returns>
  public SharableDict<TValue> With(Int64 key, TValue value) {
    var newRoot = new Destructured<TValue>(_root, key).RebuildWithNewLeafHere(value);
    return new SharableDict<TValue>(newRoot);
  }

  /// <summary>
  /// Makes a new SharableDict with the same entries as 'this', except that there is no entry for 'key'.
  /// Logically, this will be a remove operation (if this dictionary currently contains 'key') or a no-op
  /// (if it does). 'this' dictionary is unchanged.
  /// </summary>
  /// <param name="key">The key to remove</param>
  /// <returns>The resulting dictionary</returns>
  public SharableDict<TValue> Without(Int64 key) {
    var newRoot = new Destructured<TValue>(_root, key).RebuildWithoutLeafHere();
    return newRoot == _root ? this : new SharableDict<TValue>(newRoot);
  }

  /// <summary>
  /// If 'key' is present in the dictionary, stores its corresponding value in 'value' and returns true.
  /// Otherwise (if 'key' is not present in the dictionary), returns false.
  /// </summary>
  /// <param name="key">The key to look up</param>
  /// <param name="value">Storage for the looked-up value</param>
  /// <returns>True if the key was found; false otherwise.</returns>
  public bool TryGetValue(Int64 key, [MaybeNullWhen(false)] out TValue value) {
    var s = new Destructured<TValue>(_root, key);
    if (!s.Depth10.TryGetChild(s.LeafIndex, out var temp)) {
      value = default;
      return false;
    }
    value = temp;
    return true;
  }

  /// <summary>
  /// Returns true if 'key' is present in the dictionary; otherwise returns false.
  /// </summary>
  /// <param name="key">The key to look up</param>
  /// <returns>True if the key was found; false otherwise.</returns>
  public bool ContainsKey(Int64 key) {
    return TryGetValue(key, out _);
  }

  /// <summary>
  /// Calculates the difference between 'this' dictionary and 'target', represented as three resulting dictionaries.
  /// The 'added' dictionary contains entries in 'target' whose keys are not present in 'this'.
  /// The 'removed' dictionary contains entries in 'this' whose keys are not present in 'target'.
  /// The 'modified' dictionary contains entries from 'target' for keys that are present in both dictionaries, but whose values
  /// are unequal to those in 'this', as defined by the value's "Equals()" method. Note that the entries in "modified"
  /// represent how the entries look in 'target', that is after they have been modified. If your application needs to
  /// get both the 'before' and 'after' value, it will need to specifically look up the 'before' value. Example:
  ///
  /// var (added, removed, modified) = oldDict.CalcDifference(newDict)
  /// foreach (var (k, v) in added) {
  ///   ... process added k, v
  /// }
  /// foreach (var (k, v) in removed) {
  ///   ... process removed k, v
  /// }
  /// foreach (var (k, newValue) in modified) {
  ///   var found = oldDict.TryGetValue(k, out var oldValue);
  ///   assert(found);  // can't fail
  ///   ... process modified value (k, oldValue, newValue)
  /// }
  /// </summary>
  /// <param name="target">The dictionary to compare 'this' with</param>
  /// <returns>A tuple in the order (added, removed, modified) of three dictionaries.</returns>

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

  /// <summary>
  /// Gets an enumerator for the dictionary.
  /// </summary>
  /// <returns>The dictionary enumerator</returns>
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

  /// <summary>
  /// The number of entries in this dictionary.
  /// </summary>
  public int Count => _root.Count;

  public override string ToString() {
    return string.Join(", ", this.Select(kvp => $"{kvp.Key}: {kvp.Value}"));
  }

  /// <summary>
  /// For unit tests
  /// </summary>
  internal ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableNode<ImmutableLeaf<TValue>>>>>>>>>>> RootForUnitTests
   => _root;

  /// <summary>
  /// Counts distinct nodes in the tree. Used in unit testing.
  /// </summary>
  /// <returns>The number of distinct nodes in the tree</returns>
  internal int CountNodesForUnitTesting() {
    var set = new HashSet<object>(ReferenceEqualityComparer.Instance);
    _root.GatherNodesForUnitTesting(set);
    return set.Count;
  }
}
