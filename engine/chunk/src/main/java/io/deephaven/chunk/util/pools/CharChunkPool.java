package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableCharChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableCharChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

public interface CharChunkPool {

    default ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableCharChunk(capacity);
            }

            @Override
            public <ATTR extends Any> void giveWritableChunk(@NotNull final WritableChunk<ATTR> writableChunk) {
                giveWritableCharChunk(writableChunk.asWritableCharChunk());
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableCharChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableChunk(@NotNull final ResettableReadOnlyChunk<ATTR> resettableChunk) {
                giveResettableCharChunk(resettableChunk.asResettableCharChunk());
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableCharChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableWritableChunk(@NotNull final ResettableWritableChunk<ATTR> resettableWritableChunk) {
                giveResettableWritableCharChunk(resettableWritableChunk.asResettableWritableCharChunk());
            }
        };
    }
    <ATTR extends Any> WritableCharChunk<ATTR> takeWritableCharChunk(int capacity);

    void giveWritableCharChunk(@NotNull WritableCharChunk<?> writableCharChunk);

    <ATTR extends Any> ResettableCharChunk<ATTR> takeResettableCharChunk();

    void giveResettableCharChunk(@NotNull ResettableCharChunk resettableCharChunk);

    <ATTR extends Any> ResettableWritableCharChunk<ATTR> takeResettableWritableCharChunk();

    void giveResettableWritableCharChunk(@NotNull ResettableWritableCharChunk resettableWritableCharChunk);
}
