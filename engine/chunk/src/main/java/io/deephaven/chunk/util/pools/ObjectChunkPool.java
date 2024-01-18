package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ResettableObjectChunk;
import io.deephaven.chunk.ResettableReadOnlyChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.ResettableWritableObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

public interface ObjectChunkPool {
    default ChunkPool asChunkPool() {
        return new ChunkPool() {
            @Override
            public <ATTR extends Any> WritableChunk<ATTR> takeWritableChunk(final int capacity) {
                return takeWritableObjectChunk(capacity);
            }

            @Override
            public <ATTR extends Any> void giveWritableChunk(@NotNull final WritableChunk<ATTR> writableChunk) {
                giveWritableObjectChunk(writableChunk.asWritableObjectChunk());
            }

            @Override
            public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> takeResettableChunk() {
                return takeResettableObjectChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableChunk(@NotNull final ResettableReadOnlyChunk<ATTR> resettableChunk) {
                giveResettableObjectChunk(resettableChunk.asResettableObjectChunk());
            }

            @Override
            public <ATTR extends Any> ResettableWritableChunk<ATTR> takeResettableWritableChunk() {
                return takeResettableWritableObjectChunk();
            }

            @Override
            public <ATTR extends Any> void giveResettableWritableChunk(@NotNull final ResettableWritableChunk<ATTR> resettableWritableChunk) {
                giveResettableWritableObjectChunk(resettableWritableChunk.asResettableWritableObjectChunk());
            }
        };
    }

    <TYPE, ATTR extends Any> WritableObjectChunk<TYPE, ATTR> takeWritableObjectChunk(int capacity);

    void giveWritableObjectChunk(@NotNull WritableObjectChunk<?, ?> writableObjectChunk);

    <TYPE, ATTR extends Any> ResettableObjectChunk<TYPE, ATTR> takeResettableObjectChunk();

    void giveResettableObjectChunk(@NotNull ResettableObjectChunk<?, ?> resettableObjectChunk);

    <TYPE, ATTR extends Any> ResettableWritableObjectChunk<TYPE, ATTR> takeResettableWritableObjectChunk();

    void giveResettableWritableObjectChunk(@NotNull ResettableWritableObjectChunk<?, ?> resettableWritableObjectChunk);
}
