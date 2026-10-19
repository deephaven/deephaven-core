//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.snapshot;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.LazySnapshotTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;

import java.util.Map;

public class SnapshotInternalListener extends BaseTable.ListenerImpl {
    private final QueryTable triggerTable;
    private final boolean lazySnapshot;
    private final Table snapshotTable;
    private long snapshotPrevLength;
    private boolean dataCopied;
    private final QueryTable result;
    private final Map<String, SingleValueColumnSource<?>> resultTriggerColumns;
    private final Map<String, WritableColumnSource<?>> resultBaseColumns;
    private final WritableColumnSource<?>[] objectBaseColumns;
    private final Map<String, ? extends ColumnSource<?>> triggerStampColumns;
    private final Map<String, ChunkSource.WithPrev<? extends Values>> snapshotDataColumns;
    private final TrackingWritableRowSet resultRowSet;

    public SnapshotInternalListener(QueryTable triggerTable,
            boolean lazySnapshot,
            Table snapshotTable,
            QueryTable result,
            Map<String, SingleValueColumnSource<?>> resultTriggerColumns,
            Map<String, WritableColumnSource<?>> resultBaseColumns,
            TrackingWritableRowSet resultRowSet) {
        super("snapshot " + result.getDefinition().getColumnNameSet(), triggerTable, result);
        this.triggerTable = triggerTable;
        this.result = result;
        this.lazySnapshot = lazySnapshot;
        this.snapshotTable = snapshotTable;
        this.snapshotPrevLength = 0;
        this.resultTriggerColumns = resultTriggerColumns;
        this.resultBaseColumns = resultBaseColumns;
        this.objectBaseColumns = SnapshotUtils.objectColumns(resultBaseColumns);
        this.resultRowSet = resultRowSet;
        if (snapshotTable.isRefreshing()) {
            manage(snapshotTable);
        }
        triggerStampColumns = SnapshotUtils.generateTriggerStampColumns(triggerTable);
        snapshotDataColumns = SnapshotUtils.generateSnapshotDataColumns(snapshotTable);
    }

    @Override
    public void onUpdate(final TableUpdate upstream) {
        doSnapshot(true, false);
    }

    public void doSnapshot(final boolean notifyListeners, final boolean usePrev) {
        if (lazySnapshot) {
            ((LazySnapshotTable) snapshotTable).refreshForSnapshot();
        }

        // Populate stamp columns from the triggering table
        if (triggerTable.getRowSet().isEmpty()) {
            SnapshotUtils.setNullStampColumns(triggerStampColumns, resultTriggerColumns);
        } else {
            SnapshotUtils.copyStampColumns(triggerStampColumns, triggerTable.getRowSet().lastRowKey(),
                    resultTriggerColumns);
        }
        final TrackingRowSet currentRowSet = snapshotTable.getRowSet();
        final long snapshotSize;
        if (dataCopied && !lazySnapshot && !snapshotTable.isRefreshing()) {
            // A static snapshot table never changes, so the data copied by the first snapshot remains current. A lazy
            // snapshot table is not refreshing, but changes each time it is refreshed for a snapshot.
            snapshotSize = currentRowSet.size();
        } else {
            final RowSet snapshotRowSet = usePrev ? currentRowSet.prev() : currentRowSet;
            snapshotSize = snapshotRowSet.size();
            if (!snapshotRowSet.isEmpty()) {
                try (final RowSet destRowSet = RowSetFactory.fromRange(0, snapshotSize - 1)) {
                    SnapshotUtils.copyDataColumns(snapshotDataColumns,
                            snapshotRowSet, resultBaseColumns, destRowSet, usePrev);
                }
            }
            dataCopied = true;
        }
        if (snapshotPrevLength < snapshotSize) {
            resultRowSet.insertRange(snapshotPrevLength, snapshotSize - 1);
        } else if (snapshotPrevLength > snapshotSize) {
            resultRowSet.removeRange(snapshotSize, snapshotPrevLength - 1);
            if (objectBaseColumns.length > 0) {
                try (final RowSequence removedRows =
                        RowSequenceFactory.forRange(snapshotSize, snapshotPrevLength - 1)) {
                    SnapshotUtils.nullObjectColumns(objectBaseColumns, removedRows);
                }
            }
        }
        if (notifyListeners) {
            result.notifyListeners(new TableUpdateImpl(
                    resultRowSet.copy(), resultRowSet.copyPrev(),
                    RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        }
        snapshotPrevLength = snapshotSize;
    }

    @Override
    public boolean canExecute(final long step) {
        if (!lazySnapshot) {
            return snapshotTable.satisfied(step) && super.canExecute(step);
        }
        return super.canExecute(step);
    }

}
