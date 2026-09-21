//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.snapshot;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.ListenerRecorder;
import io.deephaven.engine.table.impl.MergedListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.util.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class SnapshotIncrementalListener extends MergedListener {
    private final QueryTable triggerTable;
    private final QueryTable resultTable;
    private final Map<String, WritableColumnSource<?>> resultColumns;
    private final ListenerRecorder baseListener;
    private final ListenerRecorder triggerListener;
    private final QueryTable baseTable;
    private final Map<String, ? extends ColumnSource<?>> triggerColumns;
    private final Map<String, ChunkSource.WithPrev<? extends Values>> snapshotDataColumns;

    private UpdateCoalescer baseUpdates;
    private final TrackingWritableRowSet lastBaseRowSet;
    private boolean firstSnapshot = true;

    public SnapshotIncrementalListener(
            QueryTable triggerTable,
            QueryTable resultTable,
            Map<String, WritableColumnSource<?>> resultColumns,
            ListenerRecorder baseListener,
            ListenerRecorder triggerListener,
            QueryTable baseTable,
            Map<String, ? extends ColumnSource<?>> triggerColumns) {
        super(Arrays.asList(baseListener, triggerListener), Collections.emptyList(), "snapshotIncremental",
                resultTable);
        this.triggerTable = triggerTable;
        this.resultTable = resultTable;
        this.resultColumns = resultColumns;
        this.baseListener = baseListener;
        this.triggerListener = triggerListener;
        this.baseTable = baseTable;
        this.triggerColumns = triggerColumns;
        this.lastBaseRowSet = RowSetFactory.empty().toTracking();
        this.snapshotDataColumns = SnapshotUtils.generateSnapshotDataColumns(baseTable);
    }

    @Override
    protected void process() {
        if (!firstSnapshot && baseListener.recordedVariablesAreValid()) {
            if (baseUpdates == null) {
                baseUpdates = new UpdateCoalescer(baseTable.getRowSet(), baseListener.getUpdate());
            } else {
                baseUpdates.update(baseListener.getUpdate());
            }
        }

        if (triggerListener.recordedVariablesAreValid()) {
            // This snapshot consumes everything accumulated since the last one, and the accumulator is discarded
            // either way, so hand it to a local that releases it on the way out.
            try (final UpdateCoalescer accumulated = baseUpdates) {
                baseUpdates = null;
                if (firstSnapshot) {
                    doFirstSnapshot(false);
                } else if (accumulated != null) {
                    doSnapshot(accumulated);
                }
            }
        }
    }

    public void doFirstSnapshot(boolean initial) {
        doRowCopy(baseTable.getRowSet());
        resultTable.getRowSet().writableCast().insert(baseTable.getRowSet());
        if (!initial) {
            resultTable.notifyListeners(resultTable.getRowSet().copy(),
                    RowSetFactory.empty(), RowSetFactory.empty());
        }
        firstSnapshot = false;
    }

    private void doSnapshot(final UpdateCoalescer accumulated) {
        lastBaseRowSet.clear();
        lastBaseRowSet.insert(baseTable.getRowSet());
        try (final RowSetShiftDataExpander expander = expand(accumulated)) {
            final RowSet baseAdded = expander.getAdded().copy();
            final RowSet baseModified = expander.getModified().copy();
            final RowSet baseRemoved = expander.getRemoved().copy();

            // baseAdded, baseModified, and baseRemoved are given away to notifyListeners below; rowsToCopy exists
            // only to drive the copy.
            try (final RowSet rowsToCopy = baseAdded.union(baseModified)) {
                doRowCopy(rowsToCopy);
            }

            resultTable.getRowSet().writableCast().update(baseAdded, baseRemoved);
            resultTable.notifyListeners(baseAdded, baseRemoved, baseModified);
        }
    }

    /**
     * Coalesce {@code accumulated} into a single update and expand its shifts. {@link UpdateCoalescer#coalesce} hands
     * us ownership of the coalesced update, and the expander copies what it needs out of it while keeping no reference
     * to it, so the update is released before we return.
     */
    private RowSetShiftDataExpander expand(final UpdateCoalescer accumulated) {
        final TableUpdate baseUpdate = accumulated.coalesce();
        try {
            return new RowSetShiftDataExpander(baseUpdate, lastBaseRowSet);
        } finally {
            baseUpdate.release();
        }
    }

    private void doRowCopy(RowSet rowSet) {
        copyRowsToResult(rowSet, triggerTable, snapshotDataColumns, triggerColumns, resultColumns);
    }

    public static void copyRowsToResult(RowSet rowsToCopy, QueryTable triggerTable,
            Map<String, ChunkSource.WithPrev<? extends Values>> snapshotDataColumns,
            Map<String, ? extends ColumnSource<?>> triggerColumns,
            Map<String, WritableColumnSource<?>> resultColumns) {
        final RowSet qtRowSet = triggerTable.getRowSet();
        if (!qtRowSet.isEmpty()) {
            SnapshotUtils.copyStampColumns(triggerColumns, qtRowSet.lastRowKey(), resultColumns, rowsToCopy);
        }
        SnapshotUtils.copyDataColumns(snapshotDataColumns, rowsToCopy, resultColumns, rowsToCopy, false);
    }
}
