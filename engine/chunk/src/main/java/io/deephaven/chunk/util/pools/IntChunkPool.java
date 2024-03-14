//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkPool and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableIntChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableIntChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

public interface IntChunkPool {

    ChunkPool asChunkPool();

    <ATTR extends Any> WritableIntChunk<ATTR> takeWritableIntChunk(int capacity);

    void giveWritableIntChunk(@NotNull WritableIntChunk<?> writableIntChunk);

    <ATTR extends Any> ResettableIntChunk<ATTR> takeResettableIntChunk();

    void giveResettableIntChunk(@NotNull ResettableIntChunk<?> resettableIntChunk);

    <ATTR extends Any> ResettableWritableIntChunk<ATTR> takeResettableWritableIntChunk();

    void giveResettableWritableIntChunk(@NotNull ResettableWritableIntChunk<?> resettableWritableIntChunk);
}
