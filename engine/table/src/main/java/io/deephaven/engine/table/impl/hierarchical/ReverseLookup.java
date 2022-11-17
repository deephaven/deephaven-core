/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.hierarchical;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

/**
 * Tool to identify the row key that corresponds to a given logical key for hierarchical table maintenance and display.
 */
public interface ReverseLookup {

    /**
     * Gets the row key value where {@code key} exists in the table, or the {@link #noEntryValue()} if {@code key} is
     * not found in the table.
     *
     * @param key A single (boxed) value for single-column keys, or an array of (boxed) values for compound keys
     * @return The row key where {@code key} exists in the table
     */
    long get(Object key);

    /**
     * Gets the row key value where {@code key} previously existed in the table, or the {@link #noEntryValue()} if
     * {@code key} was not found in the table.
     *
     * @param key A single (boxed) value for single-column keys, or an array of (boxed) values for compound keys
     * @return The row key where {@code key} previously existed in the table
     */
    long getPrev(Object key);

    /**
     * @return The value that will be returned from {@link #get(Object)} or {@link #getPrev(Object)} if no entry exists
     *         for a given key
     */
    default long noEntryValue() {
        return NULL_ROW_KEY;
    }
}
