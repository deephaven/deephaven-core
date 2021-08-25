package io.deephaven.db.util;

import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.DynamicTable;
import io.deephaven.db.v2.InstrumentedShiftAwareListener;
import io.deephaven.util.annotations.ScriptApi;

/**
 * A simple listener that prints out each update received from a table.
 *
 * <p>
 * This can be used to debug the performance of a query by attaching to various tables in an effort to understand the
 * update pattern. Optionally, you can also print out the head of the table.
 * </p>
 *
 * <p>
 * Output is directed to stdout, thus this should not be enabled in production queries.
 * </p>
 *
 * <p>
 * After you are finished, call the {@link #stop()} method to remove this listener from the source table.
 * </p>
 */
@ScriptApi
public class PrintListener extends InstrumentedShiftAwareListener {
    private final String description;
    private final DynamicTable table;
    private final int rowCount;

    /**
     * Create a PrintListener attached to the given table.
     *
     * @param description the description (for use in each print statement)
     * @param table the table to attach to
     */
    @ScriptApi
    public PrintListener(final String description, final DynamicTable table) {
        this(description, table, 0);
    }

    /**
     * Create a PrintListener attached to the given table.
     *
     * @param description the description (for use in each print statement)
     * @param table the table to attach to
     * @param rowCount how many rows to print out on each update
     */
    @ScriptApi
    public PrintListener(final String description, final DynamicTable table, final int rowCount) {
        super("PrintListener " + description);
        this.description = description;
        this.table = table;
        this.rowCount = rowCount;
        table.listenForUpdates(this);
    }

    @Override
    public void onUpdate(final Update upstream) {
        System.out.println("Update: " + description + ": " + table.size() + "\nAdded rows: " + upstream.added.size()
                + ", Removed rows: " + upstream.removed.size() + ", Modified Rows: " + upstream.modified.size()
                + ", Shifted Rows: " + upstream.shifted.getEffectiveSize() + "\nUpdate:" + upstream);
        if (rowCount > 0) {
            TableTools.showWithIndex(table, rowCount);
        }
    }

    @Override
    public void onFailureInternal(Throwable originalException,
            io.deephaven.db.v2.utils.UpdatePerformanceTracker.Entry sourceEntry) {
        System.out.println("Error for: " + description);
        originalException.printStackTrace();
    }

    /**
     * Remove this listener from the table.
     */
    public void stop() {
        table.removeUpdateListener(this);
    }
}
