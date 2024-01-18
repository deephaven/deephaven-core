/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkPool and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableFloatChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableFloatChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

public interface FloatChunkPool {

    ChunkPool asChunkPool();

    <ATTR extends Any> WritableFloatChunk<ATTR> takeWritableFloatChunk(int capacity);

    void giveWritableFloatChunk(@NotNull WritableFloatChunk<?> writableFloatChunk);

    <ATTR extends Any> ResettableFloatChunk<ATTR> takeResettableFloatChunk();

    void giveResettableFloatChunk(@NotNull ResettableFloatChunk<?> resettableFloatChunk);

    <ATTR extends Any> ResettableWritableFloatChunk<ATTR> takeResettableWritableFloatChunk();

    void giveResettableWritableFloatChunk(@NotNull ResettableWritableFloatChunk<?> resettableWritableFloatChunk);
}
