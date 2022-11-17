package io.deephaven.engine.table.impl.by;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

/**
 * Tool to identify the aggregation result row key (also row position) from a logical key representing a set of values
 * for the aggregation's group-by columns.
 */
public interface AggregationRowLookup {

    /**
     * Re-usable empty key, for use in (trivial) reverse lookups against no-key aggregations.
     */
    Object EMPTY_KEY = new Object();

    /**
     * Re-usable unknown row constant to serve as the default return value for {@link #noEntryValue()}.
     */
    int DEFAULT_UNKNOWN_ROW = (int) NULL_ROW_KEY;

    /**
     * Gets the row key value where {@code key} exists in the aggregation result table, or the {@link #noEntryValue()}
     * if {@code key} is not found in the table.
     * <p>
     * This serves to map group-by column values to the row position (also row key) in the result table. Missing keys
     * will map to {@link #noEntryValue()}.
     * <p>
     * Keys are specified as follows:
     * <dl>
     * <dt>No group-by columns</dt>
     * <dd>"Empty" keys are signified by the {@link AggregationRowLookup#EMPTY_KEY} object, or any zero-length
     * {@code Object[]}</dd>
     * <dt>One group-by column</dt>
     * <dd>Singular keys are (boxed, if needed) objects</dd>
     * <dt>Multiple group-by columns</dt>
     * <dd>Compound keys are {@code Object[]} of (boxed, if needed) objects, in the order of the aggregation's group-by
     * columns</dd>
     * </p>
     * <p>
     * Reinterpretation for key fields, if needed, will be applied internally by the row lookup function.
     *
     * @param key A single (boxed) value for single-column keys, or an array of (boxed) values for compound keys
     * @return The row key where {@code key} exists in the table
     */
    int get(Object key);

    /**
     * @return The value that will be returned from {@link #get(Object)} if no entry exists for a given key
     */
    default int noEntryValue() {
        return DEFAULT_UNKNOWN_ROW;
    }
}
