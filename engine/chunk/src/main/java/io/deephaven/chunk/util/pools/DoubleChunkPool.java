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

    default ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableDoubleChunk(capacity);
            }

            @Override
            public <ATTR extends Any> void giveWritableChunk(@NotNull final WritableChunk<ATTR> writableChunk) {
                giveWritableDoubleChunk(writableChunk.asWritableDoubleChunk());
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableDoubleChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableChunk(@NotNull final ResettableReadOnlyChunk<ATTR> resettableChunk) {
                giveResettableDoubleChunk(resettableChunk.asResettableDoubleChunk());
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableDoubleChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableWritableChunk(@NotNull final ResettableWritableChunk<ATTR> resettableWritableChunk) {
                giveResettableWritableDoubleChunk(resettableWritableChunk.asResettableWritableDoubleChunk());
            }
        };
    }
    <ATTR extends Any> WritableDoubleChunk<ATTR> takeWritableDoubleChunk(int capacity);

    void giveWritableDoubleChunk(@NotNull WritableDoubleChunk<?> writableDoubleChunk);

    <ATTR extends Any> ResettableDoubleChunk<ATTR> takeResettableDoubleChunk();

    void giveResettableDoubleChunk(@NotNull ResettableDoubleChunk resettableDoubleChunk);

    <ATTR extends Any> ResettableWritableDoubleChunk<ATTR> takeResettableWritableDoubleChunk();

    void giveResettableWritableDoubleChunk(@NotNull ResettableWritableDoubleChunk resettableWritableDoubleChunk);
}
