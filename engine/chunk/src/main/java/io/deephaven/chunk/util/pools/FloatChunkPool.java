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

    default ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableFloatChunk(capacity);
            }

            @Override
            public <ATTR extends Any> void giveWritableChunk(@NotNull final WritableChunk<ATTR> writableChunk) {
                giveWritableFloatChunk(writableChunk.asWritableFloatChunk());
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableFloatChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableChunk(@NotNull final ResettableReadOnlyChunk<ATTR> resettableChunk) {
                giveResettableFloatChunk(resettableChunk.asResettableFloatChunk());
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableFloatChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableWritableChunk(@NotNull final ResettableWritableChunk<ATTR> resettableWritableChunk) {
                giveResettableWritableFloatChunk(resettableWritableChunk.asResettableWritableFloatChunk());
            }
        };
    }
    <ATTR extends Any> WritableFloatChunk<ATTR> takeWritableFloatChunk(int capacity);

    void giveWritableFloatChunk(@NotNull WritableFloatChunk<?> writableFloatChunk);

    <ATTR extends Any> ResettableFloatChunk<ATTR> takeResettableFloatChunk();

    void giveResettableFloatChunk(@NotNull ResettableFloatChunk resettableFloatChunk);

    <ATTR extends Any> ResettableWritableFloatChunk<ATTR> takeResettableWritableFloatChunk();

    void giveResettableWritableFloatChunk(@NotNull ResettableWritableFloatChunk resettableWritableFloatChunk);
}
