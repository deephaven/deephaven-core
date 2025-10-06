//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//

namespace Deephaven.Dh_NetClient;

public sealed class ImmutableNode<TChild> : ImmutableBase<ImmutableNode<TChild>> where TChild : ImmutableBase<TChild>, new() {
  public static readonly ImmutableNode<TChild> Empty = new();

  public override ImmutableNode<TChild> GetEmptyInstanceForThisType() => Empty;

  public static ImmutableNode<TChild> OfArray64(ReadOnlySpan<TChild> children) {
    var subtreeCount = 0;
    for (var i = 0; i != children.Length; ++i) {
      var child = children[i];
      subtreeCount += child.Count;
    }
    return subtreeCount == 0 ? Empty : new ImmutableNode<TChild>(subtreeCount, children);
  }

  public readonly Array64<TChild> Children;

  public ImmutableNode() : base(0) {
    // This is our hack to access the static T.Empty for type T
    var emptyChild = new TChild().GetEmptyInstanceForThisType();
    ((Span<TChild>)Children).Fill(emptyChild);
  }

  private ImmutableNode(int count, ReadOnlySpan<TChild> children) : base(count) {
    children.CopyTo(Children);
  }

  public ImmutableNode<TChild> Replace(int index, TChild newChild) {
    // If we are about to replace our only non-empty child, then canonicalize.
    if (Count == Children[index].Count && newChild.Count == 0) {
      return Empty;
    }
    var newChildren = new Array64<TChild>();
    ((ReadOnlySpan<TChild>)Children).CopyTo(newChildren);
    newChildren[index] = newChild;
    return OfArray64(newChildren);
  }

  public override (ImmutableNode<TChild>, ImmutableNode<TChild>, ImmutableNode<TChild>) CalcDifference(
    ImmutableNode<TChild> target) {
    var empty = Empty;
    if (this == target) {
      // Source and target are the same. No changes
      return (empty, empty, empty);  // added, removed, modified
    }
    if (this == empty) {
      // Relative to an empty source, everything in target was added
      return (target, empty, empty);  // added, removed, modified
    }
    if (target == empty) {
      // Relative to an empty destination, everything in src was removed
      return (empty, this, empty);  // added, removed, modified
    }

    // Need to recurse to all children to build new nodes
    Array64<TChild> addedChildren = new();
    Array64<TChild> removedChildren = new();
    Array64<TChild> modifiedChildren = new();

    var length = ((ReadOnlySpan<TChild>)Children).Length;
    for (var i = 0; i != length; ++i) {
      var (a, r, m) = Children[i].CalcDifference(target.Children[i]);
      addedChildren[i] = a;
      removedChildren[i] = r;
      modifiedChildren[i] = m;
    }

    var aResult = OfArray64(addedChildren);
    var rResult = OfArray64(removedChildren);
    var mResult = OfArray64(modifiedChildren);
    return (aResult, rResult, mResult);
  }

  public override void GatherNodesForUnitTesting(HashSet<object> nodes) {
    if (!nodes.Add(this)) {
      return;
    }
    foreach (var child in Children) {
      child.GatherNodesForUnitTesting(nodes);
    }
  }
}
