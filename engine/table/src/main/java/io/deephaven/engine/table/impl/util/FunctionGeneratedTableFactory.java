/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.ListenerRecorder;
import io.deephaven.engine.table.impl.MergedListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * An abstract table that represents the result of a function.
 * <p>
 * The table will run by regenerating the full values (using the tableGenerator Function passed in). The resultant
 * table's values are copied into the result table and appropriate listener notifications are fired.
 * <p>
 * All the rows in the output table are modified on every tick, even if no actual changes occurred. The output table
 * also has a contiguous RowSet.
 * <p>
 * The generator function must produce a V2 table, and the table definition must not change between invocations.
 * <p>
 * If you are transforming a table, you should generally prefer to use the regular table operations as opposed to this
 * factory, because they are capable of performing some operations incrementally. However, for small tables this might
 * prove to require less development effort.
 */
public class FunctionGeneratedTableFactory {

    private final Supplier<Table> tableGenerator;
    private final int refreshIntervalMs;

    private final Map<String, WritableColumnSource<?>> writableSources = new LinkedHashMap<>();
    private final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
    private final TrackingWritableRowSet rowSet;

    private long nextRefresh;

    /**
     * Create a table that refreshes based on the value of your function, automatically called every refreshIntervalMs.
     *
     * @param tableGenerator a function returning a table to copy into the output table
     * @return a ticking table (assuming sourceTables have been specified) generated by tableGenerator
     */
    public static Table create(@NotNull final Supplier<Table> tableGenerator, final int refreshIntervalMs) {
        return new FunctionGeneratedTableFactory(tableGenerator, refreshIntervalMs).getTable();
    }

    /**
     * Create a table that refreshes based on the value of your function, automatically called in a
     * dependency-respecting way when at least one of the {@code sourceTables} tick.
     * <p>
     * <em>Note</em> that the {@code tableGenerator} may access data in the {@code sourceTables} but should not perform
     * further table operations on them without careful handling. Table operations may be memoized, and it is possible
     * that a table operation will return a table created by a previous invocation of the same operation. Since that
     * result will not have been included in the {@code sourceTables}, it's not automatically treated as a dependency
     * for purposes of determining when it's safe to invoke {@code tableGenerator}, allowing races to exist between
     * accessing the operation result and that result's own update processing. It's best to include all dependencies
     * directly in {@code sourceTables}, or only compute on-demand inputs under a
     * {@link io.deephaven.engine.liveness.LivenessScope}.
     *
     * @param tableGenerator a function returning a table to copy into the output table
     * @param sourceTables The query engine does not know the details of your function inputs. If you are dependent on a
     *        ticking table tables in your tableGenerator function, you can add it to this list so that the function
     *        will be recomputed on each tick.
     * @return a ticking table (assuming sourceTables have been specified) generated by tableGenerator
     */
    public static Table create(@NotNull final Supplier<Table> tableGenerator, @NotNull final Table... sourceTables) {
        if (sourceTables.length == 0) {
            // Map the no-tables-provided case to a refreshIntervalMs==-1 function call.
            return new FunctionGeneratedTableFactory(tableGenerator, -1).getTable();
        }

        // Verify the source tables are all refreshing.
        final Collection<Table> notRefreshing =
                Arrays.stream(sourceTables).filter(t -> !t.isRefreshing()).collect(Collectors.toList());
        if (!notRefreshing.isEmpty()) {
            throw new IllegalArgumentException("All source tables must be refreshing: " + notRefreshing);
        }

        final UpdateGraph updateGraph = NotificationQueue.Dependency.getUpdateGraph(sourceTables[0], sourceTables);
        final FunctionBackedTable functionBackedTableResult;
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            final FunctionGeneratedTableFactory factory = new FunctionGeneratedTableFactory(tableGenerator, 0);
            functionBackedTableResult = factory.getTable();
            functionBackedTableResult.getUpdateGraph().checkInitiateSerialTableOperation();

            final List<ListenerRecorder> listenerRecorders = new ArrayList<>(sourceTables.length);
            for (int ii = 0; ii < sourceTables.length; ii++) {
                Table sourceTable = sourceTables[ii];
                listenerRecorders.add(
                        new ListenerRecorder("FunctionGeneratedTable_source_" + ii, sourceTable, null));
            }

            final MergedListener mergedListener = new MergedListener(
                    listenerRecorders,
                    Collections.emptyList(),
                    "FunctionGeneratedTableFactory",
                    functionBackedTableResult) {
                @Override
                protected void process() {
                    functionBackedTableResult.doRefresh();
                }
            };

            for (int ii = 0; ii < listenerRecorders.size(); ii++) {
                ListenerRecorder listenerRecorder = listenerRecorders.get(ii);
                listenerRecorder.setMergedListener(mergedListener);
                sourceTables[ii].addUpdateListener(listenerRecorder);
            }

            functionBackedTableResult.setParentListener(mergedListener);
        }

        return functionBackedTableResult;
    }

    private FunctionGeneratedTableFactory(@NotNull final Supplier<Table> tableGenerator, final int refreshIntervalMs) {
        this.tableGenerator = tableGenerator;
        this.refreshIntervalMs = refreshIntervalMs;
        nextRefresh = System.currentTimeMillis() + this.refreshIntervalMs;

        Table initialTable = tableGenerator.get();
        if (initialTable.isRefreshing()) {
            if (ExecutionContext.getContext().getUpdateGraph() != initialTable.getUpdateGraph()) {
                throw new IllegalStateException(
                        "Function-generated tables must belong to the same UpdateGraph as the creating FunctionGeneratedTableFactory.");
            }
            initialTable.getUpdateGraph().checkInitiateSerialTableOperation();
        }

        for (Map.Entry<String, ? extends ColumnSource<?>> entry : initialTable.getColumnSourceMap().entrySet()) {
            ColumnSource<?> columnSource = entry.getValue();
            final WritableColumnSource<?> memoryColumnSource = ArrayBackedColumnSource.getMemoryColumnSource(
                    0, columnSource.getType(), columnSource.getComponentType());
            columns.put(entry.getKey(), memoryColumnSource);
            writableSources.put(entry.getKey(), memoryColumnSource);
        }

        copyTable(initialTable);

        // enable prev tracking after columns are initialized
        columns.values().forEach(ColumnSource::startTrackingPrevValues);

        rowSet = RowSetFactory.flat(initialTable.size()).toTracking();
    }

    private FunctionBackedTable getTable() {
        return new FunctionBackedTable(rowSet, columns);
    }

    private long updateTable() {
        Table newTable = tableGenerator.get();
        if (newTable.isRefreshing()) {
            if (ExecutionContext.getContext().getUpdateGraph() != newTable.getUpdateGraph()) {
                throw new IllegalStateException(
                        "Function-generated tables must belong to the same UpdateGraph as the creating FunctionGeneratedTableFactory.");
            }
            if (!newTable.getUpdateGraph().satisfied(newTable.getUpdateGraph().clock().currentStep())) {
                throw new IllegalStateException("The function-generated table must be satisfied to be valid.");
            }
        }

        copyTable(newTable);

        return newTable.size();
    }

    private void copyTable(Table source) {
        final Map<String, ? extends ColumnSource<?>> sourceColumns = source.getColumnSourceMap();
        final ChunkSource.WithPrev[] sourceColumnsArray = new ChunkSource.WithPrev[sourceColumns.size()];
        final WritableColumnSource[] destColumnsArray = new WritableColumnSource[sourceColumns.size()];

        final RowSet sourceRowSet = source.getRowSet();
        int cc = 0;
        for (Map.Entry<String, ? extends ColumnSource<?>> entry : sourceColumns.entrySet()) {
            WritableColumnSource<?> destColumn = writableSources.get(entry.getKey());
            destColumn.ensureCapacity(sourceRowSet.size());
            sourceColumnsArray[cc] = entry.getValue();
            destColumnsArray[cc++] = destColumn;
        }

        // noinspection unchecked
        ChunkUtils.copyData(sourceColumnsArray, sourceRowSet, destColumnsArray,
                RowSequenceFactory.forRange(0, sourceRowSet.size() - 1),
                false);
    }

    /**
     * @implNote The constructor publishes {@code this} to the {@link UpdateGraph} and cannot be subclassed.
     */
    private final class FunctionBackedTable extends QueryTable implements Runnable {

        private volatile MergedListener parentListener;

        @ReferentialIntegrity
        private Runnable delayedErrorReference;

        private FunctionBackedTable(
                @NotNull final TrackingRowSet rowSet,
                @NotNull final Map<String, ColumnSource<?>> columns) {
            super(rowSet, columns);
            if (refreshIntervalMs >= 0) {
                setRefreshing(true);
                if (refreshIntervalMs > 0) {
                    updateGraph.addSource(this);
                }
            }
        }

        private void setParentListener(@NotNull final MergedListener parentListener) {
            addParentReference(parentListener);
            // Keep an extra reference to our parent in case we need to force its reference count to zero before we
            // destroy ourselves.
            this.parentListener = parentListener;
        }

        @Override
        public void run() {
            if (System.currentTimeMillis() < nextRefresh) {
                return;
            }
            nextRefresh = System.currentTimeMillis() + refreshIntervalMs;

            doRefresh();
        }

        private void doRefresh() {
            try {
                final long size = rowSet.size();
                final long newSize = updateTable();

                if (newSize < size) {
                    final RowSet removed = RowSetFactory.fromRange(newSize, size - 1);
                    rowSet.remove(removed);
                    final RowSet modified = rowSet.copy();
                    notifyListeners(RowSetFactory.empty(), removed, modified);
                    return;
                }
                if (newSize > size) {
                    final RowSet added = RowSetFactory.fromRange(size, newSize - 1);
                    final RowSet modified = rowSet.copy();
                    rowSet.insert(added);
                    notifyListeners(added, RowSetFactory.empty(), modified);
                    return;
                }
                if (size > 0) {
                    // no size change, just modified
                    final RowSet modified = rowSet.copy();
                    notifyListeners(RowSetFactory.empty(), RowSetFactory.empty(), modified);
                }
            } catch (Exception e) {
                // Remove this failed table from the update graph.
                if (refreshIntervalMs > 0) {
                    updateGraph.removeSource(this);
                }

                // Notify listeners that we had an issue refreshing the table.
                if (getLastNotificationStep() == updateGraph.clock().currentStep()) {
                    if (parentListener != null) {
                        parentListener.forceReferenceCountToZero();
                    }
                    delayedErrorReference = new DelayedErrorNotifier(e, this);
                } else {
                    notifyListenersOnError(e, null);
                    forceReferenceCountToZero();
                }
            }
        }

        @Override
        public void destroy() {
            super.destroy();
            if (refreshIntervalMs > 0) {
                updateGraph.removeSource(this);
            }
            if (parentListener != null) {
                parentListener.forceReferenceCountToZero();
            }
        }
    }

    private static final class DelayedErrorNotifier implements Runnable {

        private final Throwable error;
        private final UpdateGraph updateGraph;
        private final WeakReference<BaseTable<?>> tableReference;

        private DelayedErrorNotifier(@NotNull final Throwable error,
                @NotNull final BaseTable<?> table) {
            this.error = error;
            updateGraph = table.getUpdateGraph();
            tableReference = new WeakReference<>(table);
            updateGraph.addSource(this);
        }

        @Override
        public void run() {
            updateGraph.removeSource(this);

            final BaseTable<?> table = tableReference.get();
            if (table == null) {
                return;
            }

            table.notifyListenersOnError(error, null);
            table.forceReferenceCountToZero();
        }
    }
}
