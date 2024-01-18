/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkPool and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
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
