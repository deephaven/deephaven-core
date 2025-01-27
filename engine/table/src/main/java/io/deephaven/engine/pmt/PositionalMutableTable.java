package io.deephaven.engine.pmt;

import io.deephaven.engine.table.Table;

import java.util.concurrent.Future;

/**
 * This is an interface for a row (positional) based mutable table.
 * <p>
 * Operations are contained within a "bundle", which begins with a startBundle call and ends with an endBundle call.
 * <p>
 * Between the startBundle and endBundle, you may insert (null) rows, delete row, and then set cells.
 * <p>
 * The caller is responsible for providing a consistent stream of updates (i.e. if you have threads make sure they do
 * not interfere with each other's bundles).  The bundle is applied on the Deephaven UpdateGraph thread, only when
 * complete.  Multiple bundles may be applied at once, before downstream listeners (operations) are notified of any
 * changes.
 */
public interface PositionalMutableTable {
    /**
     * Add count rows at rowPosition.
     * <p>
     * Rows that are after the region to be inserted are shifted.  The size of the table always increases by count.
     * The newly allocated rows are null filled.
     *
     * @param rowPosition the position to add to (starting at zero)
     * @param count       the number of rows to add.
     */
    void addRow(long rowPosition, long count);

    /**
     * Remove count rows at rowPosition.
     * <p>
     * If rows in the middle of the table are removed, then the data afterward is shifted.  The table size is always
     * reduced by count.
     *
     * @param rowPosition the position to remove (starting at zero)
     * @param count       the number of rows to remove.
     */
    void deleteRow(long rowPosition, long count);

    /**
     * Set the value of a cell.
     *
     * @param rowPosition the row to set (starting at zero)
     * @param column      the column to set (starting at zero)
     * @param value       the value to set, this must match the type of the column.
     */
    void setCell(long rowPosition, int column, Object value);

    /**
     * Set a rectangle of data, represented by the newData table.
     * <p>
     * newData must have the same columns and types as this PositionalMutableTable.
     *
     * @param rowPosition the position to begin setting newData (starting at zero)
     * @param newData     the table of data
     */
    void set2D(long rowPosition, Table newData);

    /**
     * Begin an atomic bundle of add, delete, and set operations.
     */
    void startBundle();

    /**
     * End an atomic bundle of add, delete, and set operations.
     * <p>
     * The Future&lt;Void&gt; is returned so that you have an indication when the updates have been processed.  Just
     * because the particular bundle has been processed does not mean that all of the derivative tables have been
     * processed.  You can take the sharedLock to verify that the LTM cycle has completed.
     * <p>
     * As an alternative contract, if we wanted to ensure that all derivative tables have completed their updates, the
     * PositionalMutableTable implementation could install a TerminalNotification which would complete this future
     * after the UpdateGraph has been traveresd.
     *
     * @return a future that is completed after the PositionalMutableTable has processed these updates.
     */
    Future<Void> endBundle();
}
