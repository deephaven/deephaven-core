package io.deephaven.engine.v2.utils;

import gnu.trove.list.array.TLongArrayList;

/**
 * Factory for constructing {@link MutableRowSet row sets}.
 */
public interface RowSetFactory {

    /**
     * Get an empty {@link MutableRowSet}.
     *
     * @return A new {@link MutableRowSet} containing no rows
     */
    MutableRowSet getEmptyRowSet();

    /**
     * Get a {@link MutableRowSet} containing the specified row keys. Row keys must be nonnegative numbers.
     *
     * @param rowKeys The row keys to include
     * @return A new {@link MutableRowSet} containing the specified row keys
     */
    MutableRowSet getRowSetByValues(long... rowKeys);

    /**
     * Produce a {@link MutableRowSet} containing a single row key. Row keys must be nonnegative numbers.
     *
     * @param key The row key to include
     * @return A new {@link MutableRowSet} containing the specified row key
     */
    MutableRowSet getRowSetByValues(long key);

    /**
     * Get an {@link MutableRowSet} containing the specified keys.
     * <p>
     * The provided list is sorted, and then passed to a sequential builder.
     *
     * @param list a Trove long array list; note that this list is mutated within the method
     * @return a RowSet containing the values from list
     */
    /**
     * Get a {@link MutableRowSet} containing the specified row keys. Row keys must be nonnegative numbers.
     * 
     * @apiNote The provided list is sorted in-place and then passed to a
     *          {@link RowSetBuilderSequential}.
     *
     * @param rowKeys The row keys to include; note that this list is mutated to ensure sorted order
     * @return A new {@link MutableRowSet} containing the specified row keys
     */
    RowSet getRowSetByValues(TLongArrayList rowKeys);

    /**
     * Create a {@link RowSet} containing the continuous range [firstRowKey, lastRowKey]
     *
     * @param firstRowKey The first row key in the continuous range
     * @param lastRowKey The last row key in the continuous range
     * @return A RowSet containing the specified row key range
     */
    MutableRowSet getRowSetByRange(long firstRowKey, long lastRowKey);

    /**
     * Get a flat {@link MutableRowSet} containing the range [0, size), or an {@link #getEmptyRowSet() empty row set} if
     * the specified size is <= 0.
     *
     * @param size The size of the RowSet to create
     * @return A flat {@link MutableRowSet} containing the keys [0, size) or an empty {@link MutableRowSet} if the size
     *         is <= 0
     */
    MutableRowSet getFlatRowSet(long size);

    /**
     * @return A {@link RowSetBuilderRandom} suitable for inserting ranges in no particular order
     */
    RowSetBuilderRandom getRandomBuilder();

    /**
     * @return A {@link RowSetBuilderRandom} optimized for inserting ranges sequentially in order
     */
    RowSetBuilderSequential getSequentialBuilder();
}
