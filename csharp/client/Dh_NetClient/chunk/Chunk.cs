//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
global using BooleanChunk = Deephaven.Dh_NetClient.Chunk<bool>;
global using StringChunk = Deephaven.Dh_NetClient.Chunk<string>;
global using CharChunk = Deephaven.Dh_NetClient.Chunk<char>;
global using ByteChunk = Deephaven.Dh_NetClient.Chunk<sbyte>;
global using Int16Chunk = Deephaven.Dh_NetClient.Chunk<System.Int16>;
global using Int32Chunk = Deephaven.Dh_NetClient.Chunk<System.Int32>;
global using Int64Chunk = Deephaven.Dh_NetClient.Chunk<System.Int64>;
global using FloatChunk = Deephaven.Dh_NetClient.Chunk<float>;
global using DoubleChunk = Deephaven.Dh_NetClient.Chunk<double>;
global using DateTimeOffsetChunk = Deephaven.Dh_NetClient.Chunk<System.DateTimeOffset>;
global using DateOnlyChunk = Deephaven.Dh_NetClient.Chunk<System.DateOnly>;
global using TimeOnlyChunk = Deephaven.Dh_NetClient.Chunk<System.TimeOnly>;

namespace Deephaven.Dh_NetClient;

public abstract class Chunk(int size) {
  public int Size { get; } = size;
}

public sealed class Chunk<T> : Chunk {
  public static Chunk<T> Create(int size) {
    return new Chunk<T>(new T[size]);
  }

  public T[] Data { get; }

  private Chunk(T[] data) : base(data.Length) {
    Data = data;
  }
}
