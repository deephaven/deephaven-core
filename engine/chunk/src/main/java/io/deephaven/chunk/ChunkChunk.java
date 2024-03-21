//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;

public interface ChunkChunk<ATTR extends Any> {
    Chunk<ATTR> getChunk(int pos);

    ChunkChunk<ATTR> slice(int offset, int capacity);

    int size();

    default ByteChunkChunk<ATTR> asByteChunkChunk() {
        return (ByteChunkChunk<ATTR>) this;
    }

    default BooleanChunkChunk<ATTR> asBooleanChunkChunk() {
        return (BooleanChunkChunk<ATTR>) this;
    }

    default CharChunkChunk<ATTR> asCharChunkChunk() {
        return (CharChunkChunk<ATTR>) this;
    }

    default ShortChunkChunk<ATTR> asShortChunkChunk() {
        return (ShortChunkChunk<ATTR>) this;
    }

    default IntChunkChunk<ATTR> asIntChunkChunk() {
        return (IntChunkChunk<ATTR>) this;
    }

    default LongChunkChunk<ATTR> asLongChunkChunk() {
        return (LongChunkChunk<ATTR>) this;
    }

    default FloatChunkChunk<ATTR> asFloatChunkChunk() {
        return (FloatChunkChunk<ATTR>) this;
    }

    default DoubleChunkChunk<ATTR> asDoubleChunkChunk() {
        return (DoubleChunkChunk<ATTR>) this;
    }

    default <T> ObjectChunkChunk<T, ATTR> asObjectChunkChunk() {
        return (ObjectChunkChunk<T, ATTR>) this;
    }
}
