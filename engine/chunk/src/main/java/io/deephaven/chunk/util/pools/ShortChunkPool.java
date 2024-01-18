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

    default ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableShortChunk(capacity);
            }

            @Override
            public <ATTR extends Any> void giveWritableChunk(@NotNull final WritableChunk<ATTR> writableChunk) {
                giveWritableShortChunk(writableChunk.asWritableShortChunk());
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableShortChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableChunk(@NotNull final ResettableReadOnlyChunk<ATTR> resettableChunk) {
                giveResettableShortChunk(resettableChunk.asResettableShortChunk());
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableShortChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableWritableChunk(@NotNull final ResettableWritableChunk<ATTR> resettableWritableChunk) {
                giveResettableWritableShortChunk(resettableWritableChunk.asResettableWritableShortChunk());
            }
        };
    }
    <ATTR extends Any> WritableShortChunk<ATTR> takeWritableShortChunk(int capacity);

    void giveWritableShortChunk(@NotNull WritableShortChunk<?> writableShortChunk);

    <ATTR extends Any> ResettableShortChunk<ATTR> takeResettableShortChunk();

    void giveResettableShortChunk(@NotNull ResettableShortChunk resettableShortChunk);

    <ATTR extends Any> ResettableWritableShortChunk<ATTR> takeResettableWritableShortChunk();

    void giveResettableWritableShortChunk(@NotNull ResettableWritableShortChunk resettableWritableShortChunk);
}
