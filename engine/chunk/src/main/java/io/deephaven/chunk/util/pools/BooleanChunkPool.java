/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkPool and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableBooleanChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableBooleanChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

public interface BooleanChunkPool {

    ChunkPool asChunkPool();

    <ATTR extends Any> WritableBooleanChunk<ATTR> takeWritableBooleanChunk(int capacity);

    void giveWritableBooleanChunk(@NotNull WritableBooleanChunk<?> writableBooleanChunk);

    <ATTR extends Any> ResettableBooleanChunk<ATTR> takeResettableBooleanChunk();

    void giveResettableBooleanChunk(@NotNull ResettableBooleanChunk<?> resettableBooleanChunk);

    <ATTR extends Any> ResettableWritableBooleanChunk<ATTR> takeResettableWritableBooleanChunk();

    void giveResettableWritableBooleanChunk(@NotNull ResettableWritableBooleanChunk<?> resettableWritableBooleanChunk);
}
