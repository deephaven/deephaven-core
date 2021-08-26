package io.deephaven.db.v2.sources.chunk;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;

/**
 * {@link WritableChunk} that may have its backing storage reset to a slice of that belonging to another
 * {@link WritableChunk} or a native array.
 */
public interface ResettableChunk<ATTR_BASE extends Any> extends Chunk {

    /**
     * Reset the data and bounds of this chunk to a range or sub-range of the specified {@link WritableChunk}.
     *
     * @param other The other {@link WritableChunk}
     * @param offset The offset into other
     * @param capacity The capacity this should have after reset
     *
     * @return this
     */
    <ATTR extends ATTR_BASE> Chunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset, int capacity);

    /**
     * Reset the data and bounds of this chunk to a range or sub-range of the specified array.
     *
     * @param array The array
     * @param offset The offset into array
     * @param capacity The capacity this should have after reset
     *
     * @return this
     */
    <ATTR extends ATTR_BASE> Chunk<ATTR> resetFromArray(Object array, int offset, int capacity);

    /**
     * Reset the data and bounds of this chunk to the entire range of the specified array.
     *
     * @param array The array
     *
     * @return this
     */
    <ATTR extends ATTR_BASE> Chunk<ATTR> resetFromArray(Object array);

    /**
     * Reset this chunk to empty storage.
     */
    <ATTR extends ATTR_BASE> Chunk<ATTR> clear();
}
