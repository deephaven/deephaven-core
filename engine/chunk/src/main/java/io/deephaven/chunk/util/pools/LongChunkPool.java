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

    default ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableLongChunk(capacity);
            }

            @Override
            public <ATTR extends Any> void giveWritableChunk(@NotNull final WritableChunk<ATTR> writableChunk) {
                giveWritableLongChunk(writableChunk.asWritableLongChunk());
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableLongChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableChunk(@NotNull final ResettableReadOnlyChunk<ATTR> resettableChunk) {
                giveResettableLongChunk(resettableChunk.asResettableLongChunk());
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableLongChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableWritableChunk(@NotNull final ResettableWritableChunk<ATTR> resettableWritableChunk) {
                giveResettableWritableLongChunk(resettableWritableChunk.asResettableWritableLongChunk());
            }
        };
    }
    <ATTR extends Any> WritableLongChunk<ATTR> takeWritableLongChunk(int capacity);

    void giveWritableLongChunk(@NotNull WritableLongChunk<?> writableLongChunk);

    <ATTR extends Any> ResettableLongChunk<ATTR> takeResettableLongChunk();

    void giveResettableLongChunk(@NotNull ResettableLongChunk resettableLongChunk);

    <ATTR extends Any> ResettableWritableLongChunk<ATTR> takeResettableWritableLongChunk();

    void giveResettableWritableLongChunk(@NotNull ResettableWritableLongChunk resettableWritableLongChunk);
}
