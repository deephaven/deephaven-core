/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.util.config;

import io.deephaven.db.tables.Table;

import java.util.Map;

public interface InputTableRowSetter {
    /**
     * Set the values of the column specified by the input, filling in missing data using the
     * parameter 'table' as the previous value source. This method will be invoked asynchronously.
     * Users may use {@link #setRows(Table, int[], Map[], InputTableStatusListener)} to be notified
     * of asynchronous results.
     *
     * @param table The table to use as the previous value source
     * @param row The row index to set
     * @param values A map of column name to value to set.
     */
    default void setRow(Table table, int row, Map<String, Object> values) {
        // noinspection unchecked
        setRows(table, new int[] {row}, new Map[] {values});
    }

    /**
     * Set the values of the columns specified by the input, filling in missing data using the
     * parameter 'table' as the previous value source. This method will be invoked asynchronously.
     * Users may use {@link #setRows(Table, int[], Map[], InputTableStatusListener)} to be notified
     * of asynchronous results.
     *
     * @param table The table to use as the previous value source
     * @param rowArray The row indices to update.
     * @param valueArray The new values.
     */
    default void setRows(Table table, int[] rowArray, Map<String, Object>[] valueArray) {
        setRows(table, rowArray, valueArray, InputTableStatusListener.DEFAULT);
    }

    /**
     * Set the values of the columns specified by the input, filling in missing data using the
     * parameter 'table' as the previous value source. This method will be invoked asynchronously.
     * The input listener will be notified on success/failure
     *
     * @param table The table to use as the previous value source
     * @param rowArray The row indices to update.
     * @param valueArray The new values.
     * @param listener The listener to notify on asynchronous results.
     */
    void setRows(Table table, int[] rowArray, Map<String, Object>[] valueArray,
        InputTableStatusListener listener);

    /**
     * Add the specified row to the table. Duplicate keys will be overwritten. This method will
     * execute asynchronously. Users may use {@link #addRow(Map, boolean, InputTableStatusListener)}
     * to handle the result of the asynchronous write.
     *
     * @param values The values to write.
     */
    default void addRow(Map<String, Object> values) {
        // noinspection unchecked
        addRows(new Map[] {values});
    }

    /**
     * Add the specified rows to the table. Duplicate keys will be overwritten. This method will
     * execute asynchronously. Users may use
     * {@link #addRows(Map[], boolean, InputTableStatusListener)} to handle the asynchronous result.
     *
     * @param valueArray The values to write.
     */
    default void addRows(Map<String, Object>[] valueArray) {
        addRows(valueArray, true, InputTableStatusListener.DEFAULT);
    }

    /**
     * Add the specified row to the table, optionally overwriting existing keys. This method will
     * execute asynchronously, the input listener will be notified on success/failure.
     *
     * @param valueArray The value to write.
     * @param allowEdits Should pre-existing keys be overwritten?
     * @param listener The listener to report asynchronous result to.
     */
    default void addRow(Map<String, Object> valueArray, boolean allowEdits,
        InputTableStatusListener listener) {
        // noinspection unchecked
        addRows(new Map[] {valueArray}, allowEdits, listener);
    }

    /**
     * Add the specified rows to the table, optionally overwriting existing keys. This method will
     * execute asynchronously, the input listener will be notified on success/failure.
     *
     * @param valueArray The values to write.
     * @param allowEdits Should pre-existing keys be overwritten?
     * @param listener The listener to report asynchronous results to.
     */
    void addRows(Map<String, Object>[] valueArray, boolean allowEdits,
        InputTableStatusListener listener);
}
