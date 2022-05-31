package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;

public interface WritableChunkChunk<ATTR extends Any> extends ChunkChunk<ATTR> {
    default WritableByteChunkChunk<ATTR> asWritableByteChunkChunk() {
        return (WritableByteChunkChunk<ATTR>) this;
    }

    default WritableBooleanChunkChunk<ATTR> asWritableBooleanChunkChunk() {
        return (WritableBooleanChunkChunk<ATTR>) this;
    }

    default WritableCharChunkChunk<ATTR> asWritableCharChunkChunk() {
        return (WritableCharChunkChunk<ATTR>) this;
    }

    default WritableShortChunkChunk<ATTR> asWritableShortChunkChunk() {
        return (WritableShortChunkChunk<ATTR>) this;
    }

    default WritableIntChunkChunk<ATTR> asWritableIntChunkChunk() {
        return (WritableIntChunkChunk<ATTR>) this;
    }

    default WritableLongChunkChunk<ATTR> asWritableLongChunkChunk() {
        return (WritableLongChunkChunk<ATTR>) this;
    }

    default WritableFloatChunkChunk<ATTR> asWritableFloatChunkChunk() {
        return (WritableFloatChunkChunk<ATTR>) this;
    }

    default WritableDoubleChunkChunk<ATTR> asWritableDoubleChunkChunk() {
        return (WritableDoubleChunkChunk<ATTR>) this;
    }

    default <T> WritableObjectChunkChunk<T, ATTR> asWritableObjectChunkChunk() {
        return (WritableObjectChunkChunk<T, ATTR>) this;
    }

    WritableChunk<ATTR> getWritableChunk(int pos);

    void setWritableChunk(int pos, WritableChunk<ATTR> chunk);

    @Override
    WritableChunkChunk<ATTR> slice(int offset, int capacity);
}
