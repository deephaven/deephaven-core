package io.deephaven.db.v2.sources.chunkcolumnsource;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableChunk;

/**
 * An immutable ColumnSource that is backed by chunks.
 *
 * The owner of the column source may append chunks to with the addChunk call.
 *
 *
 * @param <T> the data type of the column source
 */
public interface ChunkColumnSource<T> extends ColumnSource<T> {
    /**
     * Create a new ChunkColumnSource for the given chunk type and data type.
     *
     * @param chunkType the type of chunk
     * @param dataType the datatype for the newly created column source
     *
     * @return an empty ChunkColumnSource
     */
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

    /**
     * Append a chunk of data to this column source.
     *
     * @param chunk the chunk of data to add
     */
    void addChunk(WritableChunk<? extends Attributes.Values> chunk);

    /**
     *  Reset the column source to be ready for reuse.
     *
     *  Clear will discard the currently held chunks.  This should not be called if a table will continue to reference
     *  the column source; as it violates the immutability contract.
     */
    void clear();
}
