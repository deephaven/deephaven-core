/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkPool and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableShortChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableShortChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

public interface ShortChunkPool {

    ChunkPool asChunkPool();

    <ATTR extends Any> WritableShortChunk<ATTR> takeWritableShortChunk(int capacity);

    void giveWritableShortChunk(@NotNull WritableShortChunk<?> writableShortChunk);

    <ATTR extends Any> ResettableShortChunk<ATTR> takeResettableShortChunk();

    void giveResettableShortChunk(@NotNull ResettableShortChunk<?> resettableShortChunk);

    <ATTR extends Any> ResettableWritableShortChunk<ATTR> takeResettableWritableShortChunk();

    void giveResettableWritableShortChunk(@NotNull ResettableWritableShortChunk<?> resettableWritableShortChunk);
}
