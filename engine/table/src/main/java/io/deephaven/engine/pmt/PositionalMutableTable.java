//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.pmt;

import io.deephaven.engine.table.Table;

import java.util.concurrent.Future;

/**
 * This is an interface for a row (positional) based mutable table.
 * <p>
 * Operations are contained within a "transaction", which begins with a startTransaction call and ends with an
 * endTransaction call.
 * <p>
 * Between the startTransaction and endTransaction, you may insert (null) rows, delete row, and then set cells.
 * <p>
 * The caller is responsible for providing a consistent stream of updates (i.e. if you have threads make sure they do
 * not interfere with each other's transactions). The transaction is applied on the Deephaven UpdateGraph thread, only
 * when complete. Multiple transactions may be applied at once, before downstream listeners (operations) are notified of
 * any changes.
 */
public interface PositionalMutableTable {
    /**
     * Add count rows at rowPosition.
     * <p>
     * Rows that are after the region to be inserted are shifted. The size of the table always increases by count. The
     * newly allocated rows are null filled.
     *
     * @param rowPosition the position to add to (starting at zero)
     * @param count the number of rows to add.
     */
    void addRows(long rowPosition, long count);

    /**
     * Remove count rows at rowPosition.
     * <p>
     * If rows in the middle of the table are removed, then the data afterward is shifted. The table size is always
     * reduced by count.
     *
     * @param rowPosition the position to remove (starting at zero)
     * @param count the number of rows to remove.
     */
    void deleteRows(long rowPosition, long count);

    /**
     * Set the value of a cell.
     *
     * @param rowPosition the row to set (starting at zero)
     * @param column the column to set (starting at zero)
     * @param value the value to set, this must match the type of the column.
     */
    // TODO: make type-specific version? add way of updating multiple (noncontiguous cells), like
    // updateColumn(List<values> values, int[] indices)
    void setCell(long rowPosition, int column, Object value);

    /**
     * Set a rectangle of data, represented by the newData table.
     * <p>
     * newData must have the same columns and types as this PositionalMutableTable.
     *
     * @param rowPosition the position to begin setting newData (starting at zero)
     * @param newData the table of data
     */
    // TODO: will this add rows, or does it have to match existing size?
    void set2D(long rowPosition, Table newData);

    /**
     * Begin an atomic transaction of add, delete, and set operations.
     */
    void startTransaction();

    /**
     * End an atomic transaction of add, delete, and set operations.
     * <p>
     * The Future&lt;Void&gt; is returned so that you have an indication when the updates have been processed. Just
     * because the particular transaction has been processed does not mean that all of the derivative tables have been
     * processed. You can take the sharedLock to verify that the LTM cycle has completed.
     * <p>
     * As an alternative contract, if we wanted to ensure that all derivative tables have completed their updates, the
     * PositionalMutableTable implementation could install a TerminalNotification which would complete this future after
     * the UpdateGraph has been traveresd.
     *
     * @return a future that is completed after the PositionalMutableTable has processed these updates.
     */
    Future<Void> endTransaction();
}
