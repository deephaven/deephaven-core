//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//

using System.Diagnostics.CodeAnalysis;

namespace Deephaven.Dh_NetClient;

public sealed class ImmutableLeaf<TValue> : ImmutableBase<ImmutableLeaf<TValue>> {
  public static readonly ImmutableLeaf<TValue> Empty = new();

  public override ImmutableLeaf<TValue> GetEmptyInstanceForThisType() => Empty;

  /// <summary>
  /// This constructor is used only to make the Empty singleton. No one else should call it.
  /// We are keeping it public because our generics have a new() constraint on them, which
  /// requires that this be public.
  /// </summary>
  public ImmutableLeaf() : base(0) {
  }

  public static ImmutableLeaf<TValue> Of(Bitset64 validitySet, ReadOnlySpan<TValue> children) {
    return validitySet.IsEmpty ? Empty : new ImmutableLeaf<TValue>(validitySet.Count, validitySet, children);
  }

  public readonly Bitset64 ValiditySet;
  public readonly Array64<TValue> Children;

  private ImmutableLeaf(int count, Bitset64 validitySet, ReadOnlySpan<TValue> children) 
    : base(count) {
    ValiditySet = validitySet;
    children.CopyTo(Children);
  }

  public ImmutableLeaf<TValue> With(int index, TValue value) {
    var newVs = ValiditySet.WithElement(index);
    var newCount = newVs.Count;
    var newChildren = new Array64<TValue>();
    ((ReadOnlySpan<TValue>)Children).CopyTo(newChildren);
    newChildren[index] = value;
    return new ImmutableLeaf<TValue>(newCount, newVs, newChildren);
  }

  public ImmutableLeaf<TValue> Without(int index) {
    var newVs = ValiditySet.WithoutElement(index);
    if (newVs.IsEmpty) {
      return Empty;
    }
    var newCount = newVs.Count;
    var newChildren = new Array64<TValue>();
    ((ReadOnlySpan<TValue>)Children).CopyTo(newChildren);
    newChildren[index] = default;
    return new ImmutableLeaf<TValue>(newCount, newVs, Children);
  }

  public bool TryGetChild(int childIndex, [MaybeNullWhen(false)] out TValue child) {
    if (!ValiditySet.ContainsElement(childIndex)) {
      child = default;
      return false;
    }
    child = Children[childIndex];
    return true;
  }

  public override (ImmutableLeaf<TValue>, ImmutableLeaf<TValue>, ImmutableLeaf<TValue>) CalcDifference(
    ImmutableLeaf<TValue> target) {
    var empty = Empty;
    if (this == target) {
      // Source and target are the same. No changes
      return (empty, empty, empty); // added, removed, modified
    }
    if (this == empty) {
      // Relative to an empty source, everything in target was added
      return (target, empty, empty); // added, removed, modified
    }
    if (target == empty) {
      // Relative to an empty destination, everything in src was removed
      return (empty, this, empty); // added, removed, modified
    }

    Array64<TValue> addedValues = new();
    Array64<TValue> removedValues = new();
    Array64<TValue> modifiedValues = new();

    var addedSet = new Bitset64();
    var removedSet = new Bitset64();
    var modifiedSet = new Bitset64();

    var union = ValiditySet.Union(target.ValiditySet);

    while (union.TryExtractLowestBit(out var nextUnion, out var i)) {
      union = nextUnion;

      var selfHasBit = ValiditySet.ContainsElement(i);
      var targetHasBit = target.ValiditySet.ContainsElement(i);

      switch (selfHasBit, targetHasBit) {
        case (true, true): {
          // self && target. This is a modify (if the values are different) or a no-op (if they are the same)
          if (!Object.Equals(Children[i], target.Children[i])) {
            modifiedValues[i] = target.Children[i];
            modifiedSet = modifiedSet.WithElement(i);
          }
          break;
        }

        case (true, false): {
          removedValues[i] = Children[i];
          removedSet = removedSet.WithElement(i);
          break;
        }

        case (false, true): {
          addedValues[i] = target.Children[i];
          addedSet = addedSet.WithElement(i);
          break;
        }

        case (false, false): {
          // can't happen
          throw new Exception("Assertion failure in CalcDifference");
        }
      }
    }

    var aResult = Of(addedSet, addedValues);
    var rResult = Of(removedSet, removedValues);
    var mResult = Of(modifiedSet, modifiedValues);
    return (aResult, rResult, mResult);
  }

  public override void GatherNodesForUnitTesting(HashSet<object> nodes) {
    nodes.Add(this);
  }
}
