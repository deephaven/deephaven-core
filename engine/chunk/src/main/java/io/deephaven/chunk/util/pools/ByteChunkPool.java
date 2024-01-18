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

    default ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableByteChunk(capacity);
            }

            @Override
            public <ATTR extends Any> void giveWritableChunk(@NotNull final WritableChunk<ATTR> writableChunk) {
                giveWritableByteChunk(writableChunk.asWritableByteChunk());
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableByteChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableChunk(@NotNull final ResettableReadOnlyChunk<ATTR> resettableChunk) {
                giveResettableByteChunk(resettableChunk.asResettableByteChunk());
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableByteChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableWritableChunk(@NotNull final ResettableWritableChunk<ATTR> resettableWritableChunk) {
                giveResettableWritableByteChunk(resettableWritableChunk.asResettableWritableByteChunk());
            }
        };
    }
    <ATTR extends Any> WritableByteChunk<ATTR> takeWritableByteChunk(int capacity);

    void giveWritableByteChunk(@NotNull WritableByteChunk<?> writableByteChunk);

    <ATTR extends Any> ResettableByteChunk<ATTR> takeResettableByteChunk();

    void giveResettableByteChunk(@NotNull ResettableByteChunk resettableByteChunk);

    <ATTR extends Any> ResettableWritableByteChunk<ATTR> takeResettableWritableByteChunk();

    void giveResettableWritableByteChunk(@NotNull ResettableWritableByteChunk resettableWritableByteChunk);
}
