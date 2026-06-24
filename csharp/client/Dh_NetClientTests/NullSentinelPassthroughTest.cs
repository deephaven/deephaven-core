//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class NullSentinelPassthroughTest {
  [Test]
  public async Task SentinelsVisible() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var manager = ctx.Client.Manager;
    using var t = manager.EmptyTable(1)
      .Update(
        "NullChar = (char)null",
        "NullByte = (byte)null",
        "NullShort = (short)null",
        "NullInt = (int)null",
        "NullLong = (long)null",
        "NullFloat = (float)null",
        "NullDouble = (double)null"
      );

    var ct = t.ToClientTable();
    await Assert.That(() => AssertHasSentinel(ct, 0, DeephavenConstants.NullChar)).ThrowsNothing();
    await Assert.That(() => AssertHasSentinel(ct, 1, DeephavenConstants.NullByte)).ThrowsNothing();
    await Assert.That(() => AssertHasSentinel(ct, 2, DeephavenConstants.NullShort)).ThrowsNothing();
    await Assert.That(() => AssertHasSentinel(ct, 3, DeephavenConstants.NullInt)).ThrowsNothing();
    await Assert.That(() => AssertHasSentinel(ct, 4, DeephavenConstants.NullLong)).ThrowsNothing();
    await Assert.That(() => AssertHasSentinel(ct, 5, DeephavenConstants.NullFloat)).ThrowsNothing();
    await Assert.That(() => AssertHasSentinel(ct, 6, DeephavenConstants.NullDouble)).ThrowsNothing();
  }

  private static void AssertHasSentinel<T>(IClientTable ct, int columnIndex, T sentinel) where T : struct, IEquatable<T> {
    var rs = RowSequence.CreateSequential(Interval.OfStartAndSize(0, 1));
    var cs = ct.GetColumn(columnIndex);
    var chunk = ChunkMaker.CreateChunkFor(cs, 1);
    var nullChunk = Chunk<bool>.Create(1);

    cs.FillChunk(rs, chunk, nullChunk);

    if (!nullChunk.Data[0]) {
      throw new Exception("Expected value to be null, got non-null");
    }

    if (chunk is not Chunk<T> typedChunk) {
      throw new Exception($"Expected type {Utility.FriendlyTypeName(typeof(Chunk<T>))}, " +
        $"got type {Utility.FriendlyTypeName(chunk.GetType())}");
    }

    if (!sentinel.Equals(typedChunk.Data[0])) {
      throw new Exception($"For type {Utility.FriendlyTypeName(typeof(T))}, expected value {sentinel}, got {typedChunk.Data[0]}");
    }
  }
}