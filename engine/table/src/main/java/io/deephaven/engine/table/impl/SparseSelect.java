package io.deephaven.engine.table.impl;

import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectSparseArraySource;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.util.SafeCloseablePair;
import io.deephaven.util.thread.NamingThreadFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A simpler version of {@link Table#select} that is guaranteed to preserve the original table's RowSet.
 *
 * <p>
 * Like select, the sparseSelected table's columns will be materialized in memory. Unlike select(), sparseSelect
 * guarantees the original Table's RowSet is preserved. Formula columns are not supported, only the names of columns to
 * copy into the output table. This means that each output column is independent of every other output column, which
 * enables column-level parallelism.
 * </p>
 */
public class SparseSelect {
    /**
     * How many threads should be used for sparse select? All concurrent sparseSelect operations use the same thread
     * pool.
     *
     * <p>
     * Configured using the {@code SparseSelect.threads} property. Defaults to 1.
     * </p>
     */
    private final static int SPARSE_SELECT_THREADS =
            Configuration.getInstance().getIntegerWithDefault("SparseSelect.threads", 1);

    /**
     * What size chunk (in bytes) should be read from the input column sources and written to the output column sources?
     *
     * <p>
     * Configured using the {@code SparseSelect.chunkSize} property. Defaults to 2^16.
     * </p>
     */
    private final static int SPARSE_SELECT_CHUNK_SIZE =
            Configuration.getInstance().getIntegerWithDefault("SparseSelect.chunkSize", 1 << 16);

    private final static ExecutorService executor = SPARSE_SELECT_THREADS == 1 ? null
            : Executors.newFixedThreadPool(SPARSE_SELECT_THREADS,
                    new NamingThreadFactory(SparseSelect.class, "copyThread", true));

    private SparseSelect() {} // static use only

    /**
     * Create a new table with all columns materialized.
     *
     * @param source the input table
     *
     * @return a copy of the source table with materialized column
     */
    public static Table sparseSelect(Table source) {
        return sparseSelect(source, source.getDefinition().getColumnNamesArray());
    }

    /**
     * Create a new table with the specified columns materialized and others dropped.
     *
     * @param source the input table
     * @param columnNames the columns to copy to the output
     *
     * @return a copy of the source table with materialized column
     */
    public static Table sparseSelect(Table source, String... columnNames) {
        return sparseSelect((QueryTable) source.coalesce(), CollectionUtil.ZERO_LENGTH_STRING_ARRAY, columnNames);
    }

    /**
     * Create a new table with the specified columns materialized and others dropped.
     *
     * @param source the input table
     * @param columnNames the columns to copy to the output
     *
     * @return a copy of the source table with materialized column
     */
    public static Table sparseSelect(Table source, Collection<String> columnNames) {
        return sparseSelect((QueryTable) source.coalesce(), CollectionUtil.ZERO_LENGTH_STRING_ARRAY,
                columnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    /**
     * Create a copy of the source table with the specified columns materialized.
     *
     * Other columns are passed through to the output without changes.
     *
     * @param source the input table
     * @param columnNames the columns to materialize in the output
     *
     * @return a copy of the source table with materialized columns
     */
    public static Table partialSparseSelect(Table source, Collection<String> columnNames) {
        return partialSparseSelect(source, columnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    /**
     * Create a copy of the source table with the specified columns materialized.
     *
     * Other columns are passed through to the output without changes.
     *
     * @param source the input table
     * @param columnNames the columns to materialize in the output
     *
     * @return a copy of the source table with materialized columns
     */
    public static Table partialSparseSelect(Table source, String... columnNames) {
        final Set<String> columnsToCopy = new HashSet<>(Arrays.asList(columnNames));
        final String[] preserveColumns = source.getDefinition().getColumnStream().map(ColumnDefinition::getName)
                .filter(x -> !columnsToCopy.contains(x)).toArray(String[]::new);
        return sparseSelect((QueryTable) source.coalesce(), preserveColumns, columnNames);
    }

    private static Table sparseSelect(QueryTable source, String[] preserveColumns, String[] columnNames) {
        return QueryPerformanceRecorder.withNugget("sparseSelect(" + Arrays.toString(columnNames) + ")",
                source.sizeForInstrumentation(), () -> {
                    if (source.isRefreshing()) {
                        UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();
                    }

                    final Map<String, ColumnSource<?>> resultColumns = new LinkedHashMap<>();

                    // we copy the preserve columns to the map without changes
                    for (final String preserveColumn : preserveColumns) {
                        resultColumns.put(preserveColumn, source.getColumnSource(preserveColumn));
                    }

                    final List<ColumnSource<?>> inputSourcesList = new ArrayList<>(columnNames.length);
                    final List<SparseArrayColumnSource<?>> outputSourcesList = new ArrayList<>(columnNames.length);
                    final List<ModifiedColumnSet> modifiedColumnSets = new ArrayList<>(columnNames.length);

                    for (final String columnName : columnNames) {
                        final ColumnSource<?> inputSource = source.getColumnSource(columnName);
                        if (inputSource instanceof SparseArrayColumnSource
                                || inputSource instanceof ArrayBackedColumnSource) {
                            resultColumns.put(columnName, inputSource);
                        } else {
                            inputSourcesList.add(inputSource);
                            final SparseArrayColumnSource<?> outputSource = SparseArrayColumnSource
                                    .getSparseMemoryColumnSource(inputSource.getType(), inputSource.getComponentType());
                            outputSourcesList.add(outputSource);
                            resultColumns.put(columnName, outputSource);
                            modifiedColumnSets.add(source.newModifiedColumnSet(columnName));
                        }
                    }

                    final ColumnSource<?>[] inputSources =
                            inputSourcesList.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
                    final SparseArrayColumnSource<?>[] outputSources = outputSourcesList
                            .toArray(SparseArrayColumnSource.ZERO_LENGTH_SPARSE_ARRAY_COLUMN_SOURCE_ARRAY);

                    doCopy(source.getRowSet(), inputSources, outputSources, null);

                    final QueryTable resultTable = new QueryTable(source.getRowSet(), resultColumns);

                    if (source.isRefreshing()) {
                        outputSourcesList.forEach(ColumnSource::startTrackingPrevValues);
                        final ObjectSparseArraySource<?>[] sparseObjectSources =
                                outputSourcesList.stream().filter(x -> x instanceof ObjectSparseArraySource)
                                        .map(x -> (ObjectSparseArraySource<?>) x)
                                        .toArray(ObjectSparseArraySource[]::new);
                        source.listenForUpdates(new BaseTable.ListenerImpl(
                                "sparseSelect(" + Arrays.toString(columnNames) + ")", source, resultTable) {
                            private final ModifiedColumnSet modifiedColumnSetForUpdates =
                                    resultTable.getModifiedColumnSetForUpdates();
                            private final ModifiedColumnSet.Transformer transformer =
                                    source.newModifiedColumnSetTransformer(resultTable,
                                            resultTable.getDefinition().getColumnNamesArray());

                            @Override
                            public void onUpdate(TableUpdate upstream) {
                                final TableUpdateImpl downstream = TableUpdateImpl.copy(upstream);
                                downstream.modifiedColumnSet = modifiedColumnSetForUpdates;
                                if (sparseObjectSources.length > 0) {
                                    try (final RowSet removedOnly = upstream.removed().minus(upstream.added())) {
                                        for (final ObjectSparseArraySource<?> objectSparseArraySource : sparseObjectSources) {
                                            objectSparseArraySource.remove(removedOnly);
                                        }
                                    }
                                }

                                boolean anyModified = false;
                                boolean allModified = true;

                                final boolean[] modifiedColumns = new boolean[inputSources.length];

                                for (int cc = 0; cc < inputSources.length; ++cc) {
                                    final boolean columnModified =
                                            upstream.modifiedColumnSet().containsAny(modifiedColumnSets.get(cc));
                                    modifiedColumns[cc] = columnModified;
                                    anyModified |= columnModified;
                                    allModified &= columnModified;
                                }

                                if (anyModified) {
                                    try (final RowSet addedAndModified = upstream.added().union(upstream.modified())) {
                                        if (upstream.shifted().nonempty()) {
                                            try (final RowSet currentWithoutAddsOrModifies =
                                                    source.getRowSet().minus(addedAndModified);
                                                    final SafeCloseablePair<RowSet, RowSet> shifts = upstream.shifted()
                                                            .extractParallelShiftedRowsFromPostShiftRowSet(
                                                                    currentWithoutAddsOrModifies)) {
                                                doShift(shifts, outputSources, modifiedColumns);
                                            }
                                        }

                                        doCopy(addedAndModified, inputSources, outputSources, modifiedColumns);
                                    }
                                }

                                if (!allModified) {
                                    invert(modifiedColumns);

                                    if (upstream.shifted().nonempty()) {
                                        try (final RowSet currentWithoutAdds =
                                                source.getRowSet().minus(upstream.added());
                                                final SafeCloseablePair<RowSet, RowSet> shifts = upstream.shifted()
                                                        .extractParallelShiftedRowsFromPostShiftRowSet(
                                                                currentWithoutAdds)) {
                                            doShift(shifts, outputSources, modifiedColumns);
                                        }
                                    }

                                    doCopy(upstream.added(), inputSources, outputSources, modifiedColumns);
                                }

                                transformer.transform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet());

                                resultTable.notifyListeners(downstream);
                            }

                            private void invert(boolean[] modifiedColumns) {
                                for (int cc = 0; cc < modifiedColumns.length; ++cc) {
                                    modifiedColumns[cc] = !modifiedColumns[cc];
                                }
                            }
                        });
                    }

                    return resultTable;
                });
    }

    private static void doShift(SafeCloseablePair<RowSet, RowSet> shifts, SparseArrayColumnSource<?>[] outputSources,
            boolean[] toShift) {
        if (executor == null) {
            doShiftSingle(shifts, outputSources, toShift);
        } else {
            doShiftThreads(shifts, outputSources, toShift);
        }
    }

    private static void doCopy(RowSet addedAndModified, ColumnSource<?>[] inputSources,
            WritableColumnSource<?>[] outputSources,
            boolean[] toCopy) {
        if (executor == null) {
            doCopySingle(addedAndModified, inputSources, outputSources, toCopy);
        } else {
            doCopyThreads(addedAndModified, inputSources, outputSources, toCopy);
        }
    }

    private static void doCopySingle(RowSet addedAndModified, ColumnSource<?>[] inputSources,
            WritableColumnSource<?>[] outputSources, boolean[] toCopy) {
        final ChunkSource.GetContext[] gcs = new ChunkSource.GetContext[inputSources.length];
        final ChunkSink.FillFromContext[] ffcs = new ChunkSink.FillFromContext[inputSources.length];
        try (final SafeCloseableArray<ChunkSource.GetContext> ignored = new SafeCloseableArray<>(gcs);
                final SafeCloseableArray<ChunkSink.FillFromContext> ignored2 = new SafeCloseableArray<>(ffcs);
                final RowSequence.Iterator rsIt = addedAndModified.getRowSequenceIterator();
                final SharedContext sharedContext = SharedContext.makeSharedContext()) {
            for (int cc = 0; cc < inputSources.length; cc++) {
                if (toCopy == null || toCopy[cc]) {
                    gcs[cc] = inputSources[cc].makeGetContext(SPARSE_SELECT_CHUNK_SIZE, sharedContext);
                    ffcs[cc] = outputSources[cc].makeFillFromContext(SPARSE_SELECT_CHUNK_SIZE);
                }
            }
            while (rsIt.hasMore()) {
                sharedContext.reset();
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(SPARSE_SELECT_CHUNK_SIZE);
                for (int cc = 0; cc < inputSources.length; cc++) {
                    if (toCopy == null || toCopy[cc]) {
                        final Chunk<? extends Values> values = inputSources[cc].getChunk(gcs[cc], chunkOk);
                        outputSources[cc].fillFromChunk(ffcs[cc], values, chunkOk);
                    }
                }
            }
        }
    }

    private static void doCopyThreads(RowSet addedAndModified, ColumnSource<?>[] inputSources,
            WritableColumnSource<?>[] outputSources, boolean[] toCopy) {
        final Future<?>[] futures = new Future[inputSources.length];
        for (int columnIndex = 0; columnIndex < inputSources.length; columnIndex++) {
            if (toCopy == null || toCopy[columnIndex]) {
                final int cc = columnIndex;
                futures[cc] =
                        executor.submit(() -> doCopySource(addedAndModified, outputSources[cc], inputSources[cc]));
            }
        }
        for (int columnIndex = 0; columnIndex < inputSources.length; columnIndex++) {
            if (toCopy == null || toCopy[columnIndex]) {
                try {
                    futures[columnIndex].get();
                } catch (InterruptedException e) {
                    throw new CancellationException("Interupted in sparseSelect()", e);
                } catch (ExecutionException e) {
                    throw new RuntimeException("sparseSelect copy failed", e);
                }
            }
        }
    }

    private static void doCopySource(RowSet addedAndModified, WritableColumnSource<?> outputSource,
            ColumnSource<?> inputSource) {
        try (final RowSequence.Iterator rsIt = addedAndModified.getRowSequenceIterator();
                final ChunkSink.FillFromContext ffc =
                        outputSource.makeFillFromContext(SPARSE_SELECT_CHUNK_SIZE);
                final ChunkSource.GetContext gc = inputSource.makeGetContext(SPARSE_SELECT_CHUNK_SIZE)) {
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(SPARSE_SELECT_CHUNK_SIZE);
                final Chunk<? extends Values> values = inputSource.getChunk(gc, chunkOk);
                outputSource.fillFromChunk(ffc, values, chunkOk);
            }
        }
    }

    private static void doShiftSingle(SafeCloseablePair<RowSet, RowSet> shifts,
            SparseArrayColumnSource<?>[] outputSources,
            boolean[] toShift) {
        // noinspection unchecked
        final WritableChunk<Values>[] values = new WritableChunk[outputSources.length];
        final ChunkSource.FillContext[] fcs = new ChunkSource.FillContext[outputSources.length];
        final ChunkSink.FillFromContext[] ffcs = new ChunkSink.FillFromContext[outputSources.length];

        try (final SafeCloseableArray<WritableChunk<Values>> ignored = new SafeCloseableArray<>(values);
                final SafeCloseableArray<ChunkSource.FillContext> ignored2 = new SafeCloseableArray<>(fcs);
                final SafeCloseableArray<ChunkSink.FillFromContext> ignored3 = new SafeCloseableArray<>(ffcs);
                final SharedContext sharedContext = SharedContext.makeSharedContext();
                final RowSequence.Iterator preIt = shifts.first.getRowSequenceIterator();
                final RowSequence.Iterator postIt = shifts.second.getRowSequenceIterator()) {

            for (int cc = 0; cc < outputSources.length; cc++) {
                if (toShift == null || toShift[cc]) {
                    fcs[cc] = outputSources[cc].makeFillContext(SPARSE_SELECT_CHUNK_SIZE, sharedContext);
                    ffcs[cc] = outputSources[cc].makeFillFromContext(SPARSE_SELECT_CHUNK_SIZE);
                    values[cc] = outputSources[cc].getChunkType().makeWritableChunk(SPARSE_SELECT_CHUNK_SIZE);
                }
            }

            while (preIt.hasMore()) {
                final RowSequence preChunkOk = preIt.getNextRowSequenceWithLength(SPARSE_SELECT_CHUNK_SIZE);
                final RowSequence postChunkOk = postIt.getNextRowSequenceWithLength(SPARSE_SELECT_CHUNK_SIZE);
                for (int cc = 0; cc < outputSources.length; cc++) {
                    if (toShift == null || toShift[cc]) {
                        outputSources[cc].fillPrevChunk(fcs[cc], values[cc], preChunkOk);
                        outputSources[cc].fillFromChunk(ffcs[cc], values[cc], postChunkOk);
                    }
                }
            }
        }
    }

    private static void doShiftThreads(SafeCloseablePair<RowSet, RowSet> shifts,
            SparseArrayColumnSource<?>[] outputSources,
            boolean[] toShift) {
        final Future<?>[] futures = new Future[outputSources.length];
        for (int columnIndex = 0; columnIndex < outputSources.length; columnIndex++) {
            if (toShift == null || toShift[columnIndex]) {
                final int cc = columnIndex;
                futures[cc] = executor.submit(() -> doShiftSource(shifts, outputSources[cc]));
            }
        }
        for (int columnIndex = 0; columnIndex < outputSources.length; columnIndex++) {
            if (toShift == null || toShift[columnIndex]) {
                try {
                    futures[columnIndex].get();
                } catch (InterruptedException e) {
                    throw new CancellationException("Interupted in sparseSelect()", e);
                } catch (ExecutionException e) {
                    throw new RuntimeException("sparseSelect shift failed", e);
                }
            }
        }
    }

    private static void doShiftSource(SafeCloseablePair<RowSet, RowSet> shifts,
            SparseArrayColumnSource<?> outputSource) {
        try (final RowSequence.Iterator preIt = shifts.first.getRowSequenceIterator();
                final RowSequence.Iterator postIt = shifts.second.getRowSequenceIterator();
                final ChunkSink.FillFromContext ffc =
                        outputSource.makeFillFromContext(SPARSE_SELECT_CHUNK_SIZE);
                final ChunkSource.FillContext fc = outputSource.makeFillContext(SPARSE_SELECT_CHUNK_SIZE);
                final WritableChunk<Values> values =
                        outputSource.getChunkType().makeWritableChunk(SPARSE_SELECT_CHUNK_SIZE)) {
            while (preIt.hasMore()) {
                final RowSequence preChunkOk = preIt.getNextRowSequenceWithLength(SPARSE_SELECT_CHUNK_SIZE);
                final RowSequence postChunkOk = postIt.getNextRowSequenceWithLength(SPARSE_SELECT_CHUNK_SIZE);
                outputSource.fillPrevChunk(fc, values, preChunkOk);
                outputSource.fillFromChunk(ffc, values, postChunkOk);
            }
        }
    }
}
