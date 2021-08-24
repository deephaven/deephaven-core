package io.deephaven.db.v2.sources.chunkcolumnsource;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import org.jetbrains.annotations.NotNull;

/**
 * An immutable ColumnSource that is backed by chunks.
 * <p>
 * The owner of the column source may append chunks to with the addChunk call.
 *
 * @param <T> the data type of the column source
 */
public interface ChunkColumnSource<T> extends ColumnSource<T> {
    /**
     * Create a new ChunkColumnSource for the given chunk type and data type.
     *
     * @param chunkType the type of chunk
     * @param dataType the datatype for the newly created column source
     * @return an empty ChunkColumnSource
     */
    static ChunkColumnSource<?> make(ChunkType chunkType, Class<?> dataType) {
        return make(chunkType, dataType, (Class<?>) null);
    }

    /**
     * Create a new ChunkColumnSource for the given chunk type and data type.
     *
     * @param chunkType the type of chunk
     * @param dataType the datatype for the newly created column source
     * @param componentType the component type for the newly created column source (only applies to
     *        Objects)
     * @return an empty ChunkColumnSource
     */
    static ChunkColumnSource<?> make(ChunkType chunkType, Class<?> dataType,
        Class<?> componentType) {
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
                return new ObjectChunkColumnSource<>(dataType, componentType);
            default:
                throw new IllegalArgumentException(
                    "Can not make ChunkColumnSource of type " + chunkType);
        }
    }

    /**
     * Create a new ChunkColumnSource for the given chunk type and data type.
     *
     * @param chunkType the type of chunk
     * @param dataType the datatype for the newly created column source
     * @param sharedOffsetForData an array list representing the shared offsets for data across
     *        several ChunkColumnSources
     * @return an empty ChunkColumnSource
     */
    static ChunkColumnSource<?> make(ChunkType chunkType, Class<?> dataType,
        TLongArrayList sharedOffsetForData) {
        return make(chunkType, dataType, null, sharedOffsetForData);
    }

    /**
     * Create a new ChunkColumnSource for the given chunk type and data type.
     *
     * @param chunkType the type of chunk
     * @param dataType the datatype for the newly created column source
     * @param componentType the component type for the newly created column source (only applies to
     *        Objects)
     * @param sharedOffsetForData an array list representing the shared offsets for data across
     *        several ChunkColumnSources
     * @return an empty ChunkColumnSource
     */
    static ChunkColumnSource<?> make(ChunkType chunkType, Class<?> dataType, Class<?> componentType,
        TLongArrayList sharedOffsetForData) {
        switch (chunkType) {
            case Char:
                return new CharChunkColumnSource(sharedOffsetForData);
            case Byte:
                return new ByteChunkColumnSource(sharedOffsetForData);
            case Short:
                return new ShortChunkColumnSource(sharedOffsetForData);
            case Int:
                return new IntChunkColumnSource(sharedOffsetForData);
            case Long:
                return new LongChunkColumnSource(sharedOffsetForData);
            case Float:
                return new FloatChunkColumnSource(sharedOffsetForData);
            case Double:
                return new DoubleChunkColumnSource(sharedOffsetForData);
            case Object:
                return new ObjectChunkColumnSource<>(dataType, componentType, sharedOffsetForData);
            default:
                throw new IllegalArgumentException(
                    "Can not make ChunkColumnSource of type " + chunkType);
        }
    }

    /**
     * Append a chunk of data to this column source.
     *
     * The chunk must not be empty (i.e., the size must be greater than zero).
     *
     * @param chunk the chunk of data to add
     */
    void addChunk(@NotNull WritableChunk<? extends Attributes.Values> chunk);

    /**
     * Reset the column source to be ready for reuse.
     * <p>
     * Clear will discard the currently held chunks. This should not be called if a table will
     * continue to reference the column source; as it violates the immutability contract.
     */
    void clear();

    /**
     * Get the size of this column source (one more than the last valid index).
     *
     * @return the size of this column source
     */
    long getSize();
}
