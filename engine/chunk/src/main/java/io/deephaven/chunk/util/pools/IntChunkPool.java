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

    default ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableIntChunk(capacity);
            }

            @Override
            public <ATTR extends Any> void giveWritableChunk(@NotNull final WritableChunk<ATTR> writableChunk) {
                giveWritableIntChunk(writableChunk.asWritableIntChunk());
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableIntChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableChunk(@NotNull final ResettableReadOnlyChunk<ATTR> resettableChunk) {
                giveResettableIntChunk(resettableChunk.asResettableIntChunk());
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableIntChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableWritableChunk(@NotNull final ResettableWritableChunk<ATTR> resettableWritableChunk) {
                giveResettableWritableIntChunk(resettableWritableChunk.asResettableWritableIntChunk());
            }
        };
    }
    <ATTR extends Any> WritableIntChunk<ATTR> takeWritableIntChunk(int capacity);

    void giveWritableIntChunk(@NotNull WritableIntChunk<?> writableIntChunk);

    <ATTR extends Any> ResettableIntChunk<ATTR> takeResettableIntChunk();

    void giveResettableIntChunk(@NotNull ResettableIntChunk resettableIntChunk);

    <ATTR extends Any> ResettableWritableIntChunk<ATTR> takeResettableWritableIntChunk();

    void giveResettableWritableIntChunk(@NotNull ResettableWritableIntChunk resettableWritableIntChunk);
}
