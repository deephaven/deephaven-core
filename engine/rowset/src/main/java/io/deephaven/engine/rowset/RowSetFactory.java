//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.engine.rowset.impl.AdaptiveRowSetBuilderRandom;
import io.deephaven.engine.rowset.impl.BasicRowSetBuilderSequential;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;

/**
 * Repository of factory methods for constructing {@link WritableRowSet row sets}.
 */
public abstract class RowSetFactory {

    private RowSetFactory() {}

    /**
     * Get an empty {@link WritableRowSet}.
     *
     * @return A new {@link WritableRowSet} containing no rows
     */
    public static WritableRowSet empty() {
        return new WritableRowSetImpl();
    }

    /**
     * Get a {@link WritableRowSet} containing the specified row keys. Row keys must be nonnegative numbers.
     *
     * @param rowKeys The row keys to include
     * @return A new {@link WritableRowSet} containing the specified row keys
     */
    public static WritableRowSet fromKeys(final long... rowKeys) {
        if (rowKeys.length == 0) {
            return empty();
        }
        if (rowKeys.length == 1) {
            return fromKeys(rowKeys[0]);
        }
        final RowSetBuilderRandom indexBuilder = builderRandom();
        for (final long rowKey : rowKeys) {
            indexBuilder.addKey(rowKey);
        }
        return indexBuilder.build();
    }

    /**
     * Produce a {@link WritableRowSet} containing a single row key. Row keys must be nonnegative numbers.
     *
     * @param rowKey The row key to include
     * @return A new {@link WritableRowSet} containing the specified row key
     */
    public static WritableRowSet fromKeys(final long rowKey) {
        return fromRange(rowKey, rowKey);
    }

    /**
     * Create a {@link WritableRowSet} containing the continuous range [firstRowKey, lastRowKey].
     *
     * @param firstRowKey The first row key in the continuous range
     * @param lastRowKey The last row key in the continuous range
     * @return A new {@link WritableRowSet} containing the specified row key range
     */
    public static WritableRowSet fromRange(final long firstRowKey, final long lastRowKey) {
        return new WritableRowSetImpl(SingleRange.make(firstRowKey, lastRowKey));
    }

    /**
     * Get a flat {@link WritableRowSet} containing the row key range {@code [0, size)}, or an {@link #empty() empty row
     * set} if {@code size <= 0}.
     *
     * @param size The size of the {@link WritableRowSet} to create
     * @return A flat {@link WritableRowSet} containing the row key range {@code [0, size)} or an {@link #empty() empty
     *         row set} if the {@code size <= 0}
     */
    public static WritableRowSet flat(final long size) {
        return size <= 0 ? empty() : fromRange(0, size - 1);
    }

    /**
     * @return A {@link RowSetBuilderRandom} suitable for inserting row keys and row key ranges in no particular order
     */
    public static RowSetBuilderRandom builderRandom() {
        return new AdaptiveRowSetBuilderRandom();
    }

    /**
     * @return A {@link RowSetBuilderRandom} optimized for inserting row keys and row key ranges sequentially in order
     */
    public static RowSetBuilderSequential builderSequential() {
        return new BasicRowSetBuilderSequential();
    }
}
