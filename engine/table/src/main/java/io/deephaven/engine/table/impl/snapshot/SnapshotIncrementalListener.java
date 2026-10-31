//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.snapshot;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.ListenerRecorder;
import io.deephaven.engine.table.impl.MergedListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.SparseBlockColumnSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseablePair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SnapshotIncrementalListener extends MergedListener {
    private final QueryTable triggerTable;
    private final QueryTable resultTable;
    private final Map<String, WritableColumnSource<?>> resultColumns;
    private final ListenerRecorder baseListener;
    private final ListenerRecorder triggerListener;
    private final QueryTable baseTable;
    private final Map<String, ? extends ColumnSource<?>> triggerColumns;
    private final SnapshotUtils.DataColumnCopy dataCopy;
    private final SparseBlockColumnSource[] sparseResultColumns;
    private final WritableColumnSource<?>[] objectResultColumns;
    private final WritableColumnSource<?>[] movableResultColumns;
    private final ModifiedColumnSet[] dataColumnSets;
    private final ModifiedColumnSet.Transformer dataColumnTransformer;
    private final ModifiedColumnSet stampColumnSet;

    private UpdateCoalescer baseUpdates;
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
        final Map<String, ChunkSource.WithPrev<? extends Values>> snapshotDataColumns =
                SnapshotUtils.generateSnapshotDataColumns(baseTable);
        this.dataCopy = SnapshotUtils.dataColumnCopy(snapshotDataColumns, resultColumns);

        // a reinterpretable result column, such as an Instant column, may hold its values in a sparse primitive source
        final List<SparseBlockColumnSource> sparseColumns = new ArrayList<>(resultColumns.size());
        for (final ColumnSource<?> resultColumn : resultColumns.values()) {
            final ColumnSource<?> storage = resultColumn instanceof SparseBlockColumnSource
                    ? resultColumn
                    : ReinterpretUtils.maybeConvertToPrimitive(resultColumn);
            if (storage instanceof SparseBlockColumnSource) {
                sparseColumns.add((SparseBlockColumnSource) storage);
            }
        }
        this.sparseResultColumns = sparseColumns.toArray(SparseBlockColumnSource[]::new);
        this.objectResultColumns = SnapshotUtils.objectColumns(resultColumns);
        // shifted rows move through the primitive storage of reinterpretable columns, such as Instant columns
        this.movableResultColumns = resultColumns.values().stream()
                .map(ReinterpretUtils::maybeConvertToWritablePrimitive)
                .toArray(WritableColumnSource<?>[]::new);

        final String[] dataColumnNames = snapshotDataColumns.keySet().toArray(String[]::new);
        dataColumnSets = new ModifiedColumnSet[dataColumnNames.length];
        for (int ii = 0; ii < dataColumnNames.length; ++ii) {
            dataColumnSets[ii] = baseTable.newModifiedColumnSet(dataColumnNames[ii]);
        }
        dataColumnTransformer = baseTable.newModifiedColumnSetTransformer(resultTable, dataColumnNames);
        stampColumnSet = resultTable.newModifiedColumnSet(triggerColumns.keySet().toArray(String[]::new));
    }

    @Override
    protected void process() {
        if (!firstSnapshot && baseListener.recordedVariablesAreValid()) {
            if (baseUpdates == null) {
                baseUpdates = new UpdateCoalescer(baseTable.getRowSet().prev(), baseListener.getUpdate());
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

    @Override
    protected void destroy() {
        super.destroy();
        getUpdateGraph().runWhenIdle(() -> {
            if (baseUpdates != null) {
                baseUpdates.close();
                baseUpdates = null;
            }
        });
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

    /**
     * Apply the base updates accumulated since the last snapshot to the result. The result's row set is the base's row
     * set as of the last snapshot, which is the row set {@code accumulated} began from, so the coalesced update applies
     * to the result directly: shifted rows move, and only added and modified rows are copied from the base and stamped.
     */
    private void doSnapshot(final UpdateCoalescer accumulated) {
        final TableUpdate baseUpdate = accumulated.coalesce();
        try (final SafeCloseable ignored = baseUpdate::release) {
            final TrackingWritableRowSet resultRowSet = resultTable.getRowSet().writableCast();
            final RowSetShiftData shifted = baseUpdate.shifted();

            // the keys that held a row before this snapshot and may hold none after it: the removed rows, and the rows
            // within the shifted ranges
            try (final WritableRowSet vacated = baseUpdate.removed().copy()) {
                if (shifted.nonempty()) {
                    final RowSetBuilderSequential preShiftBuilder = RowSetFactory.builderSequential();
                    final RowSetBuilderSequential postShiftBuilder = RowSetFactory.builderSequential();
                    final int shiftCount = shifted.size();
                    for (int ii = 0; ii < shiftCount; ++ii) {
                        final long delta = shifted.getShiftDelta(ii);
                        preShiftBuilder.appendRange(shifted.getBeginRange(ii), shifted.getEndRange(ii));
                        postShiftBuilder.appendRange(shifted.getBeginRange(ii) + delta,
                                shifted.getEndRange(ii) + delta);
                    }
                    try (final RowSet preShiftRanges = preShiftBuilder.build();
                            final RowSet postShiftRanges = postShiftBuilder.build();
                            final WritableRowSet remainingShifted = postShiftRanges.intersect(baseTable.getRowSet())) {
                        // every row that remains moves, including the modified rows, whose unmodified columns are not
                        // copied from the base below
                        remainingShifted.remove(baseUpdate.added());
                        try (final SafeCloseablePair<RowSet, RowSet> moved = SafeCloseablePair.downcast(
                                shifted.extractParallelShiftedRowsFromPostShiftRowSet(remainingShifted))) {
                            moveRows(moved.first, moved.second);
                        }
                        try (final RowSet shiftedFrom = preShiftRanges.intersect(resultRowSet)) {
                            vacated.insert(shiftedFrom);
                        }
                    }
                }

                // added and modified rows take the current stamp; modified rows copy only the columns that changed
                try (final RowSet stampedRows = baseUpdate.added().union(baseUpdate.modified())) {
                    copyStampsToResult(stampedRows, triggerTable, triggerColumns, resultColumns);
                }
                ChunkUtils.copyData(dataCopy.sources(), baseUpdate.added(), dataCopy.destinations(),
                        baseUpdate.added(), false);
                if (baseUpdate.modified().isNonempty()) {
                    final ModifiedColumnSet baseModifiedColumns = baseUpdate.modifiedColumnSet();
                    int numModified = 0;
                    for (final ModifiedColumnSet dataColumnSet : dataColumnSets) {
                        if (baseModifiedColumns.containsAny(dataColumnSet)) {
                            ++numModified;
                        }
                    }
                    // noinspection unchecked
                    final ChunkSource.WithPrev<? extends Values>[] modifiedSources =
                            new ChunkSource.WithPrev[numModified];
                    final WritableColumnSource<?>[] modifiedDestinations = new WritableColumnSource[numModified];
                    int mi = 0;
                    for (int ii = 0; ii < dataColumnSets.length; ++ii) {
                        if (baseModifiedColumns.containsAny(dataColumnSets[ii])) {
                            modifiedSources[mi] = dataCopy.sources()[ii];
                            modifiedDestinations[mi++] = dataCopy.destinations()[ii];
                        }
                    }
                    ChunkUtils.copyData(modifiedSources, baseUpdate.modified(), modifiedDestinations,
                            baseUpdate.modified(), false);
                }

                resultRowSet.remove(baseUpdate.removed());
                shifted.apply(resultRowSet);
                resultRowSet.insert(baseUpdate.added());
                Assert.eq(resultRowSet.size(), "resultRowSet.size()", baseTable.size(), "baseTable.size()");

                vacated.remove(resultRowSet);
                SnapshotUtils.nullObjectColumns(objectResultColumns, vacated);
                SparseBlockColumnSource.clearBlocksWithoutLiveRows(vacated, resultRowSet, sparseResultColumns);
            }

            final ModifiedColumnSet modifiedColumns = resultTable.getModifiedColumnSetForUpdates();
            dataColumnTransformer.clearAndTransform(baseUpdate.modifiedColumnSet(), modifiedColumns);
            if (baseUpdate.modified().isNonempty() && !stampColumnSet.empty()) {
                modifiedColumns.setAll(stampColumnSet);
            }
            resultTable.notifyListeners(new TableUpdateImpl(baseUpdate.added().copy(), baseUpdate.removed().copy(),
                    baseUpdate.modified().copy(), baseUpdate.shifted(), modifiedColumns));
        }
    }

    /**
     * Move the values of every result column from {@code preShiftKeys} to the parallel {@code postShiftKeys}. The
     * values are read as of the start of the cycle, so the order in which overlapping keys are written does not matter.
     */
    private void moveRows(final RowSet preShiftKeys, final RowSet postShiftKeys) {
        if (preShiftKeys.isEmpty()) {
            return;
        }
        final int chunkSize = (int) Math.min(preShiftKeys.size(), ChunkUtils.COPY_DATA_CHUNK_SIZE);
        for (final WritableColumnSource<?> column : movableResultColumns) {
            try (final ChunkSource.FillContext fillContext = column.makeFillContext(chunkSize);
                    final ChunkSink.FillFromContext fillFromContext = column.makeFillFromContext(chunkSize);
                    final WritableChunk<Values> chunk = column.getChunkType().makeWritableChunk(chunkSize);
                    final RowSequence.Iterator preIter = preShiftKeys.getRowSequenceIterator();
                    final RowSequence.Iterator postIter = postShiftKeys.getRowSequenceIterator()) {
                while (preIter.hasMore()) {
                    final RowSequence preKeys = preIter.getNextRowSequenceWithLength(chunkSize);
                    final RowSequence postKeys = postIter.getNextRowSequenceWithLength(chunkSize);
                    column.fillPrevChunk(fillContext, chunk, preKeys);
                    column.fillFromChunk(fillFromContext, chunk, postKeys);
                }
            }
        }
    }

    private void doRowCopy(RowSet rowSet) {
        copyRowsToResult(rowSet, triggerTable, dataCopy, triggerColumns, resultColumns);
    }

    /**
     * Copy the base data and the current stamp into {@code rowsToCopy} of the result.
     *
     * @param dataCopy The base data sources and the result columns that receive them
     */
    public static void copyRowsToResult(RowSet rowsToCopy, QueryTable triggerTable,
            SnapshotUtils.DataColumnCopy dataCopy,
            Map<String, ? extends ColumnSource<?>> triggerColumns,
            Map<String, WritableColumnSource<?>> resultColumns) {
        copyStampsToResult(rowsToCopy, triggerTable, triggerColumns, resultColumns);
        ChunkUtils.copyData(dataCopy.sources(), rowsToCopy, dataCopy.destinations(), rowsToCopy, false);
    }

    private static void copyStampsToResult(RowSet rowsToStamp, QueryTable triggerTable,
            Map<String, ? extends ColumnSource<?>> triggerColumns,
            Map<String, WritableColumnSource<?>> resultColumns) {
        final RowSet qtRowSet = triggerTable.getRowSet();
        if (qtRowSet.isEmpty()) {
            SnapshotUtils.setNullStampColumns(triggerColumns, resultColumns, rowsToStamp);
        } else {
            SnapshotUtils.copyStampColumns(triggerColumns, qtRowSet.lastRowKey(), resultColumns, rowsToStamp);
        }
    }
}
