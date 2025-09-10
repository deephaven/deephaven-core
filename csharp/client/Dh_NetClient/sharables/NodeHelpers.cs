//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
namespace Deephaven.Dh_NetClient;

using System.Runtime.CompilerServices;

[InlineArray(64)]
public struct Array64<T> {
  public T Item;

  public int Length => 64;
}
