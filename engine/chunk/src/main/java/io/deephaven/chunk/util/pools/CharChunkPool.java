//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableCharChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableCharChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

public interface CharChunkPool {

    ChunkPool asChunkPool();

    <ATTR extends Any> WritableCharChunk<ATTR> takeWritableCharChunk(int capacity);

    void giveWritableCharChunk(@NotNull WritableCharChunk<?> writableCharChunk);

    <ATTR extends Any> ResettableCharChunk<ATTR> takeResettableCharChunk();

    void giveResettableCharChunk(@NotNull ResettableCharChunk<?> resettableCharChunk);

    <ATTR extends Any> ResettableWritableCharChunk<ATTR> takeResettableWritableCharChunk();

    void giveResettableWritableCharChunk(@NotNull ResettableWritableCharChunk<?> resettableWritableCharChunk);
}
