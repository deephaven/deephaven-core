/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkPool and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableLongChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableLongChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

public interface LongChunkPool {

    ChunkPool asChunkPool();

    <ATTR extends Any> WritableLongChunk<ATTR> takeWritableLongChunk(int capacity);

    void giveWritableLongChunk(@NotNull WritableLongChunk<?> writableLongChunk);

    <ATTR extends Any> ResettableLongChunk<ATTR> takeResettableLongChunk();

    void giveResettableLongChunk(@NotNull ResettableLongChunk<?> resettableLongChunk);

    <ATTR extends Any> ResettableWritableLongChunk<ATTR> takeResettableWritableLongChunk();

    void giveResettableWritableLongChunk(@NotNull ResettableWritableLongChunk<?> resettableWritableLongChunk);
}
