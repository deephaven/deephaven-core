package io.deephaven.db.v2.sources.chunkcolumnsource;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkType;

public interface ChunkColumnSource<T> extends ColumnSource<T> {
    static ChunkColumnSource<?> make(ChunkType chunkType, Class<?> dataType) {
        switch (chunkType) {
            case Char:
                return new CharChunkColumnSource();
            case Byte:
                return new ByteChunkColumnSource();
            case Short:
                return new ShortChunkColumnSource();
            case Int:
                return new IntChunkColumnSource();
            case Long:
                return new LongChunkColumnSource();
            case Float:
                return new FloatChunkColumnSource();
            case Double:
                return new DoubleChunkColumnSource();
            case Object:
                return new ObjectChunkColumnSource<>(dataType);
            default:
                throw new IllegalArgumentException("Can not make ChunkColumnSource of type " + chunkType);
        }
    }

    void addChunk(Chunk<? extends Attributes.Values> chunk);
    void clear();
}
