//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.snapshot;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.ListenerRecorder;
import io.deephaven.engine.table.impl.MergedListener;
import io.deephaven.engine.table.impl.LazySnapshotTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;

import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Takes the snapshots of a non-incremental {@code snapshotWhen}. When the trigger is refreshing, a
 * {@link ListenerRecorder} on the trigger, and one on the snapshot table when it is refreshing and not lazy, feed this
 * listener; a failure of either fails the result. With a static trigger there are no recorders, and the only snapshot
 * is the initial one taken by {@link #doSnapshot(boolean, boolean)}.
 */
public class SnapshotInternalListener extends MergedListener {
    private final QueryTable triggerTable;
    private final boolean lazySnapshot;
    private final Table snapshotTable;
    private long snapshotPrevLength;
    private boolean dataCopied;
    private final Map<String, SingleValueColumnSource<?>> resultTriggerColumns;
    private final Map<String, WritableColumnSource<?>> resultBaseColumns;
    private final WritableColumnSource<?>[] objectBaseColumns;
    private final Map<String, ? extends ColumnSource<?>> triggerStampColumns;
    private final Map<String, ChunkSource.WithPrev<? extends Values>> snapshotDataColumns;
    private final TrackingWritableRowSet resultRowSet;
    private final ListenerRecorder triggerRecorder;

    /**
     * @param triggerRecorder the recorder on the trigger table, or {@code null} if the trigger is static
     * @param snapshotRecorder the recorder on the snapshot table, or {@code null} if the trigger is static or the
     *        snapshot table is static or lazy
     */
    public SnapshotInternalListener(QueryTable triggerTable,
            boolean lazySnapshot,
            Table snapshotTable,
            QueryTable result,
            Map<String, SingleValueColumnSource<?>> resultTriggerColumns,
            Map<String, WritableColumnSource<?>> resultBaseColumns,
            TrackingWritableRowSet resultRowSet,
            @Nullable ListenerRecorder triggerRecorder,
            @Nullable ListenerRecorder snapshotRecorder) {
        super(recorders(triggerRecorder, snapshotRecorder), Collections.emptyList(),
                "snapshot " + result.getDefinition().getColumnNameSet(), result);
        this.triggerTable = triggerTable;
        this.lazySnapshot = lazySnapshot;
        this.snapshotTable = snapshotTable;
        this.snapshotPrevLength = 0;
        this.resultTriggerColumns = resultTriggerColumns;
        this.resultBaseColumns = resultBaseColumns;
        this.objectBaseColumns = SnapshotUtils.objectColumns(resultBaseColumns);
        this.resultRowSet = resultRowSet;
        this.triggerRecorder = triggerRecorder;
        if (triggerRecorder != null && snapshotRecorder == null && snapshotTable.isRefreshing()) {
            // a refreshing lazy snapshot table has no recorder, but is read on every trigger tick
            manage(snapshotTable);
        }
        triggerStampColumns = SnapshotUtils.generateTriggerStampColumns(triggerTable);
        snapshotDataColumns = SnapshotUtils.generateSnapshotDataColumns(snapshotTable);
    }

    private static List<ListenerRecorder> recorders(
            @Nullable final ListenerRecorder triggerRecorder,
            @Nullable final ListenerRecorder snapshotRecorder) {
        if (triggerRecorder == null) {
            return Collections.emptyList();
        }
        return snapshotRecorder == null ? List.of(triggerRecorder) : List.of(triggerRecorder, snapshotRecorder);
    }

    @Override
    protected void process() {
        if (triggerRecorder.recordedVariablesAreValid()) {
            doSnapshot(true, false);
        }
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

}
