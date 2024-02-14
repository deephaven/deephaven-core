/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkPool and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableByteChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableByteChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

public interface ByteChunkPool {

    ChunkPool asChunkPool();

    <ATTR extends Any> WritableByteChunk<ATTR> takeWritableByteChunk(int capacity);

    void giveWritableByteChunk(@NotNull WritableByteChunk<?> writableByteChunk);

    <ATTR extends Any> ResettableByteChunk<ATTR> takeResettableByteChunk();

    void giveResettableByteChunk(@NotNull ResettableByteChunk<?> resettableByteChunk);

    <ATTR extends Any> ResettableWritableByteChunk<ATTR> takeResettableWritableByteChunk();

    void giveResettableWritableByteChunk(@NotNull ResettableWritableByteChunk<?> resettableWritableByteChunk);
}
