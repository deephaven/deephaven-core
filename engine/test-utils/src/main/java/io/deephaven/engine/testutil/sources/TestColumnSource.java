//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;

/**
 * A column source that is used within tests that we can add and remove values from at arbitrary locations.
 */
public interface TestColumnSource<T> {
    /**
     * Remove the given index from the column source.
     * 
     * @param rowSet the keys to remove
     */
    void remove(RowSet rowSet);

    /**
     * Add the chunk of data, parallel to index to this column source.
     *
     * <p>
     * For TestColumnSources with primitive types, the chunk of data must either be of the appropriate primitive type or
     * an {@link ObjectChunk} containing boxed values.
     * </p>
     *
     * @param rowSet the index locations to add the data
     * @param data the data to add
     */
    void add(final RowSet rowSet, Chunk<Values> data);

    /**
     * Move data within the column source.
     *
     * @param startKeyInclusive the first key to shift
     * @param endKeyInclusive the last key to shift
     * @param shiftDelta how far to shift the data (either positive or negative)
     */
    void shift(long startKeyInclusive, long endKeyInclusive, long shiftDelta);
}
