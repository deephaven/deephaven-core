//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableObjectChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.ResettableWritableObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

public interface ObjectChunkPool {
    ChunkPool asChunkPool();

    <TYPE, ATTR extends Any> WritableObjectChunk<TYPE, ATTR> takeWritableObjectChunk(int capacity);

    void giveWritableObjectChunk(@NotNull WritableObjectChunk<?, ?> writableObjectChunk);

    <TYPE, ATTR extends Any> ResettableObjectChunk<TYPE, ATTR> takeResettableObjectChunk();

    void giveResettableObjectChunk(@NotNull ResettableObjectChunk<?, ?> resettableObjectChunk);

    <TYPE, ATTR extends Any> ResettableWritableObjectChunk<TYPE, ATTR> takeResettableWritableObjectChunk();

    void giveResettableWritableObjectChunk(@NotNull ResettableWritableObjectChunk<?, ?> resettableWritableObjectChunk);
}
