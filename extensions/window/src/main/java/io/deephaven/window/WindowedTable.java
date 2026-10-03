package io.deephaven.window;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.ConstituentDependency;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

public class WindowedTable extends PartitionedTableImpl {
    private static final String CONSTITUENT_COL = "__CONSTITUENT__";
    private static final String WINDOW_COL = "__WINDOW__";
    private static final String WINDOW_LOWER_BIN =
            WINDOW_COL + " = lowerBin(%s, %d)";
    private static final String WINDOW_UPPER_BIN =
            WINDOW_COL + " = upperBin(%s, %d)";

    private WindowingListener listener;

    /**
     * Create a WindowedTable instance.
     *
     * @param input The input table to be windowed.
     * @param timestampColumnName The name of the timestamp column.
     * @param upperBin Whether to use upper bins for windowing.
     * @param windowPeriod The duration of each window.
     * @param windowCount The number of windows to maintain.
     * @return A WindowedTable instance.
     */
    public static WindowedTable of(@NotNull final Table input,
            @NotNull final String timestampColumnName,
            @NotNull final boolean upperBin,
            @NotNull final Duration windowPeriod,
            @NotNull final long windowCount) {
        // This is useless on a non-refreshing table, so lets enforce that here.
        Assert.eqTrue(input.isRefreshing(), "need refreshing table");

        // This is also useless on a non-blink table, since the upstreams will just
        // hold onto the data forever. It is also useful for us to assume Blink
        // semantics so just enforce this here.
        Assert.eqTrue(
                input.getAttribute(Table.BLINK_TABLE_ATTRIBUTE).equals(Boolean.TRUE),
                "need blink table");

        // Start off by creating our window column that will allow us to split up
        // incoming data into the various output tables we need to manage.
        String windowExp = String.format(WINDOW_LOWER_BIN, timestampColumnName,
                windowPeriod.toNanos());
        if (upperBin) {
            windowExp = String.format(WINDOW_UPPER_BIN, timestampColumnName,
                    windowPeriod.toNanos());
        }

        // Execute the either lower/upper bin call on the table, and then partition
        // it by the new column. There by creating our "input" table used by the
        // listener to copy the newly created tables into a managable form.
        final Table windowed = input.updateView(windowExp);
        final Table inputParts =
                windowed.removeBlink()
                        .partitionedAggBy(List.of(), true, null, WINDOW_COL)
                        .table();

        // Verify we are working with what we think we are working with table wise.
        final TableDefinition partsDef = TableDefinition.of(
                ColumnDefinition.ofTime(WINDOW_COL),
                ColumnDefinition.fromGenericType(CONSTITUENT_COL, QueryTable.class));
        Assert.eqTrue(inputParts.getDefinition().equalsIgnoreOrder(partsDef),
                "definitions mismatch");

        // Grab our input and create our output column sources.
        final ColumnSource<Instant> inputWindows =
                inputParts.getColumnSource(WINDOW_COL);
        final ColumnSource<QueryTable> inputTables =
                inputParts.getColumnSource(CONSTITUENT_COL);
        final WritableColumnSource<Instant> outputWindows =
                ArrayBackedColumnSource.getMemoryColumnSource(Instant.class, null);
        final WritableColumnSource<QueryTable> outputTables =
                ArrayBackedColumnSource.getMemoryColumnSource(QueryTable.class, null);

        // Ensure our new column sources are fully setup and ready to go.
        outputWindows.startTrackingPrevValues();
        outputTables.startTrackingPrevValues();

        // Create a new empty rowset, and create our output table.
        final TrackingWritableRowSet rowSet = RowSetFactory.empty().toTracking();
        final QueryTable outputParts = new QueryTable(
                partsDef, rowSet,
                Map.of(WINDOW_COL, outputWindows, CONSTITUENT_COL, outputTables));

        // Create the listener to handle copying tables into our output.
        final WindowingListener listener = new WindowingListener(
                inputParts, outputParts, rowSet, inputWindows, inputTables,
                outputWindows, outputTables, upperBin, windowPeriod, windowCount);

        // Install the constituent dependency between the listener
        // maintaining the table and the table itself.
        ConstituentDependency.install(outputParts, listener);

        // Finally create the actual WindowTable which is really just a
        // PartitionedTable in disguise.
        final TableDefinition constituentDef = windowed.getDefinition();
        return new WindowedTable(listener, outputParts, List.of(WINDOW_COL), true,
                CONSTITUENT_COL, constituentDef, true, true);
    }

    protected WindowedTable(@NotNull final WindowingListener listener,
            @NotNull final Table table,
            @NotNull final Collection<String> keyColumnNames,
            final boolean uniqueKeys,
            @NotNull final String constituentColumnName,
            @NotNull final TableDefinition constituentDefinition,
            final boolean constituentChangesPermitted,
            final boolean validateConstituents) {
        super(table, keyColumnNames, uniqueKeys, constituentColumnName,
                constituentDefinition, constituentChangesPermitted,
                validateConstituents);
        this.listener = listener;
    }

    /**
     * Close the WindowedTable and its associated listener.
     */
    public synchronized void close() {
        if (this.listener != null) {
            this.listener.close();
            this.listener = null;
        }
    }
}
