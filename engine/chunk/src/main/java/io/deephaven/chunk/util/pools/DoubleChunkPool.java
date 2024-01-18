/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkPool and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableDoubleChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableDoubleChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

public interface DoubleChunkPool {

    ChunkPool asChunkPool();

    <ATTR extends Any> WritableDoubleChunk<ATTR> takeWritableDoubleChunk(int capacity);

    void giveWritableDoubleChunk(@NotNull WritableDoubleChunk<?> writableDoubleChunk);

    <ATTR extends Any> ResettableDoubleChunk<ATTR> takeResettableDoubleChunk();

    void giveResettableDoubleChunk(@NotNull ResettableDoubleChunk<?> resettableDoubleChunk);

    <ATTR extends Any> ResettableWritableDoubleChunk<ATTR> takeResettableWritableDoubleChunk();

    void giveResettableWritableDoubleChunk(@NotNull ResettableWritableDoubleChunk<?> resettableWritableDoubleChunk);
}
