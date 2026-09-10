//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import com.google.common.collect.Sets;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.select.setinclusion.SetInclusionKernel;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * A where filter that extracts a set of inclusion or exclusion keys from a set table.
 * <p>
 * Each time the set table ticks, the entire where filter is recalculated.
 */
public class DynamicWhereFilter extends WhereFilterLivenessArtifactImpl
        implements NotificationAwareDependency, HasParentPerformanceIds, NoPredicatePushdown {

    private static final int CHUNK_SIZE = 1 << 16;

    private final MatchPair[] sourceToSetColumnNamePairs;
    private final boolean inclusion;

    /**
     * The set table, kernel and update listener, shared with every copy of this filter. See {@link SharedSetKernel}.
     */
    private final SharedSetKernel sharedSet;

    private List<Object> staticSetLookupKeys;

    private ColumnSource<?>[] sourceKeyColumns;
    private TupleSource<Object> sourceKeySource;
    /**
     * The optimal data index for this filter.
     */
    private @Nullable DataIndex sourceDataIndex;
    private int @Nullable [] tupleToIndexMap;
    private int @Nullable [] indexToTupleMap;

    private RecomputeListener listener;
    private QueryTable resultTable;

    /**
     * Construct a DynamicWhereFilter with key values from given set table. The set table may be static or refreshing.
     *
     * @param setTable the table containing the inclusion or exclusion keys
     * @param inclusion when true, rows matching the values in the set table are included in the results. When false,
     *        matching rows are excluded.
     * @param sourceToSetColumnNamePairs the mapping of source table column names to set table column names for the keys
     */
    public DynamicWhereFilter(
            @NotNull final Table setTable,
            final boolean inclusion,
            final MatchPair... sourceToSetColumnNamePairs) {
        this(SharedSetKernel.create(setTable, sourceToSetColumnNamePairs), inclusion, sourceToSetColumnNamePairs);
    }

    /**
     * "Copy constructor", sharing the set table, kernel and update listener of the filter being copied. A filter is
     * copied for each operation that uses it, and once per constituent table by a {@code PartitionedTable} proxy, so
     * rebuilding the set for each copy would repeat that work and leave a listener per copy on the set table.
     */
    private DynamicWhereFilter(
            @NotNull final SharedSetKernel sharedSet,
            final boolean inclusion,
            final MatchPair... sourceToSetColumnNamePairs) {
        this.sourceToSetColumnNamePairs = sourceToSetColumnNamePairs;
        this.inclusion = inclusion;
        this.sharedSet = sharedSet;
        manage(sharedSet);
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateGraph;
    }

    @Override
    public SafeCloseable beginOperation(@NotNull final Table sourceTable) {
        if (sourceKeySource != null) {
            throw new IllegalStateException("Inputs already initialized, use copy() instead of re-using a WhereFilter");
        }
        getUpdateGraph(this, sourceTable);
        final String[] keyColumnNames = MatchPair.getLeftColumns(sourceToSetColumnNamePairs);
        sourceKeyColumns = Arrays.stream(sourceToSetColumnNamePairs)
                .map(mp -> sourceTable.getColumnSource(mp.leftColumn())).toArray(ColumnSource[]::new);
        try (final SafeCloseable ignored = sourceTable.isRefreshing() ? LivenessScopeStack.open() : null) {
            sourceDataIndex = optimalIndex(sourceTable, keyColumnNames);
            if (sourceDataIndex != null) {
                if (sourceDataIndex.isRefreshing()) {
                    manage(sourceDataIndex);
                }
                computeTupleIndexMaps();
            }
        }

        final ColumnSource<?>[] reinterpretedSourceKeyColumns = Arrays.stream(sourceKeyColumns)
                .map(ReinterpretUtils::maybeConvertToPrimitive)
                .toArray(ColumnSource[]::new);
        final Class<?>[] setKeyTypes = sharedSet.setKeyTypes();
        for (int ki = 0; ki < setKeyTypes.length; ++ki) {
            if (setKeyTypes[ki] != reinterpretedSourceKeyColumns[ki].getType()) {
                throw new IllegalArgumentException(String.format(
                        "Reinterpreted key type mismatch: (set key type) %s != %s (source key type)",
                        setKeyTypes[ki], reinterpretedSourceKeyColumns[ki].getType()));
            }
        }
        sourceKeySource = TupleSourceFactory.makeTupleSource(reinterpretedSourceKeyColumns);

        if (!sharedSet.isRefreshing() // Set table is static
                && staticSetLookupKeys == null // We haven't already computed the lookup keys
                && sourceDataIndex != null // We might use the lookup keys if we compute them
                && sourceDataIndex.isRefreshing() // We might use the lookup keys more than once
                && sourceKeyColumns.length > 1 // Making a lookup key is more complicated than boxing a primitive
        ) {
            // Convert the tuples in liveValues to be lookup keys in the sourceDataIndex.
            staticSetLookupKeys = new ArrayList<>(sharedSet.kernel().size());
            final int indexKeySize = sourceDataIndex.keyColumns().length;
            if (indexKeySize > 1) {
                final Function<Object, Object> keyMappingFunction = indexKeySize == keyColumnNames.length
                        ? tupleToFullKeyMappingFunction()
                        : tupleToPartialKeyMappingFunction();

                sharedSet.kernel().iterator().forEachRemaining(key -> {
                    final Object[] lookupKey = (Object[]) keyMappingFunction.apply(key);
                    // Store a copy because the mapping function returns the same array each invocation.
                    staticSetLookupKeys.add(Arrays.copyOf(lookupKey, indexKeySize));
                });
            } else {
                final int keyOffset = indexToTupleMap == null ? 0 : indexToTupleMap[0];
                sharedSet.kernel().iterator().forEachRemaining(
                        key -> staticSetLookupKeys.add(sourceKeySource.exportElement(key, keyOffset)));
            }
        }

        return () -> {
        };
    }

    /**
     * Returns the optimal data index for the supplied table, or null if no index is available. The ideal index would
     * contain all key columns but a partial match is also acceptable.
     */
    @Nullable
    private static DataIndex optimalIndex(final Table inputTable, final String[] keyColumnNames) {
        final DataIndexer indexer = DataIndexer.existingOf(inputTable.getRowSet());
        if (indexer == null) {
            return null;
        }

        final Set<ColumnSource<?>> columnSources = Arrays.stream(keyColumnNames)
                .map(inputTable::getColumnSource)
                .collect(Collectors.toSet());

        // Find a full index if one exists
        final DataIndex fullIndex = indexer.getDataIndex(columnSources);
        if (fullIndex != null) {
            return fullIndex;
        }

        return LivenessScopeStack.computeEnclosed(() -> Sets.powerSet(columnSources).stream()
                .filter(subset -> !subset.isEmpty() && subset.size() < columnSources.size())
                .map(indexer::getDataIndex)
                .filter(Objects::nonNull)
                .max(Comparator.comparingLong(dataIndex -> dataIndex.table().size()))
                .orElse(null),
                inputTable.isRefreshing(), (final DataIndex result) -> result != null && result.isRefreshing());
    }

    /**
     * Calculates mappings from the offset of a {@link ColumnSource} in the {@code sourceDataIndex} to the offset of the
     * corresponding {@link ColumnSource} in the key sources from the set or source table of a DynamicWhereFilter
     * ({@code indexToTupleMap}, as well as the reverse ({@code tupleToIndexMap}). This allows for mapping keys from the
     * {@link SharedSetKernel kernel} to keys in the {@link #sourceDataIndex}.
     */
    private void computeTupleIndexMaps() {
        assert sourceDataIndex != null;

        if (sourceDataIndex.keyColumns().length == 1 && sourceKeyColumns.length == 1) {
            // Trivial mapping, no need to compute anything.
            return;
        }

        final ColumnSource<?>[] dataIndexSources = sourceDataIndex
                .keyColumnNamesByIndexedColumn()
                .keySet()
                .toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);

        // Bi-directional mapping (note that the sizes can be different, e.g. partial matching).
        final int[] tupleToIndexMap = new int[sourceKeyColumns.length];
        final int[] indexToTupleMap = new int[dataIndexSources.length];

        // Fill with -1 to indicate no mapping. This value will remain in the case of a partial index to indicate that
        // the key column is not in the index, thereby poisoning any mistaken attempt to use the mapping.
        Arrays.fill(tupleToIndexMap, -1);

        boolean sameOrder = true;

        // The tuples will be in sourceKeyColumns order (same as set table key columns order). We need to find the
        // dataIndex offset for each key source.
        for (int ii = 0; ii < sourceKeyColumns.length; ++ii) {
            for (int jj = 0; jj < dataIndexSources.length; ++jj) {
                if (sourceKeyColumns[ii] == dataIndexSources[jj]) {
                    tupleToIndexMap[ii] = jj;
                    indexToTupleMap[jj] = ii;
                    sameOrder &= ii == jj;
                    break;
                }
            }
        }

        // Return null if the map is the identity map
        this.tupleToIndexMap = sameOrder ? null : tupleToIndexMap;
        this.indexToTupleMap = sameOrder ? null : indexToTupleMap;
    }

    @NotNull
    private Function<Object, Object> tupleToFullKeyMappingFunction() {
        final Object[] keysInDataIndexOrder = new Object[sourceKeyColumns.length];
        if (tupleToIndexMap == null) {
            return (final Object tupleKey) -> {
                sourceKeySource.exportAllTo(keysInDataIndexOrder, tupleKey);
                return keysInDataIndexOrder;
            };
        }
        return (final Object tupleKey) -> {
            sourceKeySource.exportAllTo(keysInDataIndexOrder, tupleKey, tupleToIndexMap);
            return keysInDataIndexOrder;
        };
    }

    @NotNull
    private Function<Object, Object> tupleToPartialKeyMappingFunction() {
        assert sourceDataIndex != null;

        final int partialKeySize = sourceDataIndex.keyColumns().length;

        // This function is not needed when the partial key is a single column and should not be called.
        Assert.gt(partialKeySize, "partialKeySize", 1);

        final Object[] keysInDataIndexOrder = new Object[partialKeySize];
        if (indexToTupleMap == null) {
            return (final Object tupleKey) -> {
                for (int ii = 0; ii < partialKeySize; ++ii) {
                    keysInDataIndexOrder[ii] = sourceKeySource.exportElement(tupleKey, ii);
                }
                return keysInDataIndexOrder;
            };
        } else {
            return (final Object tupleKey) -> {
                for (int ii = 0; ii < partialKeySize; ++ii) {
                    keysInDataIndexOrder[ii] = sourceKeySource.exportElement(tupleKey, indexToTupleMap[ii]);
                }
                return keysInDataIndexOrder;
            };
        }
    }

    @Override
    public List<String> getColumns() {
        return Arrays.asList(MatchPair.getLeftColumns(sourceToSetColumnNamePairs));
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @Override
    public void init(@NotNull final TableDefinition tableDefinition) {}

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table,
            final boolean usePrev) {
        if (sourceDataIndex != null) {
            // Use previous size when filtering with previous values, so the heuristic is consistent with the data
            // we are about to read.
            final long indexTableSize = usePrev
                    ? sourceDataIndex.table().getRowSet().sizePrev()
                    : sourceDataIndex.table().getRowSet().size();
            final long threshold = (long) (indexTableSize / QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD);
            if (selection.size() <= threshold) {
                return filterLinear(selection, inclusion, usePrev);
            }
            // Does our index contain every key column?
            if (sourceDataIndex.keyColumnNames().size() == sourceKeyColumns.length) {
                return filterFullIndex(selection, usePrev);
            }
            return filterPartialIndex(selection, usePrev);
        }
        return filterLinear(selection, inclusion, usePrev);
    }

    /**
     * Apply {@code action} to each key, abandoning the enclosing snapshot attempt if the set changes underneath us; see
     * {@link SharedSetKernel#kernel()}. The check is made once per {@value #CHUNK_SIZE} keys, the same granularity as
     * the linear path's per-chunk check, and once more at the end so that no torn tail goes unchecked. Harmless for a
     * static set or a static lookup key list, whose generation never changes.
     *
     * @param keys The keys to iterate
     * @param kernelGeneration The value {@link SharedSetKernel#beginRead()} returned before {@code keys} was obtained
     * @param action What to do with each key
     */
    private void forEachKernelKey(
            @NotNull final Iterator<Object> keys,
            final long kernelGeneration,
            @NotNull final Consumer<Object> action) {
        int sinceCheck = 0;
        while (keys.hasNext()) {
            action.accept(keys.next());
            if (++sinceCheck == CHUNK_SIZE) {
                sharedSet.failIfChangedSince(kernelGeneration);
                sinceCheck = 0;
            }
        }
        sharedSet.failIfChangedSince(kernelGeneration);
    }

    @NotNull
    private WritableRowSet filterFullIndex(@NotNull final RowSet selection, final boolean usePrev) {
        Assert.neqNull(sourceDataIndex, "sourceDataIndex");

        final WritableRowSet filtered = inclusion ? RowSetFactory.empty() : selection.copy();
        // An abandoned attempt must not take its result with it; the reads below routinely throw to force a retry.
        try {
            // noinspection DataFlowIssue
            final DataIndex.RowKeyLookup rowKeyLookup = sourceDataIndex.rowKeyLookup();
            final ColumnSource<RowSet> rowSetColumn = sourceDataIndex.rowSetColumn();

            final long kernelGeneration = sharedSet.beginRead();
            final Iterator<Object> values;
            final Function<Object, Object> keyMappingFunction;
            if (staticSetLookupKeys != null) {
                values = staticSetLookupKeys.iterator();
                keyMappingFunction = Function.identity();
            } else if (sourceKeyColumns.length == 1) {
                values = sharedSet.kernel().iterator();
                keyMappingFunction = Function.identity();
            } else {
                values = sharedSet.kernel().iterator();
                keyMappingFunction = tupleToFullKeyMappingFunction();
            }

            forEachKernelKey(values, kernelGeneration, key -> {
                final Object mappedKey = keyMappingFunction.apply(key);
                final long rowKey = rowKeyLookup.apply(mappedKey, usePrev);
                final RowSet rowSet = usePrev ? rowSetColumn.getPrev(rowKey) : rowSetColumn.get(rowKey);
                if (rowSet != null) {
                    if (inclusion) {
                        try (final RowSet intersected = rowSet.intersect(selection)) {
                            filtered.insert(intersected);
                        }
                    } else {
                        filtered.remove(rowSet);
                    }
                }
            });
        } catch (final Throwable t) {
            filtered.close();
            throw t;
        }
        return filtered;
    }

    @NotNull
    private WritableRowSet filterPartialIndex(@NotNull final RowSet selection, final boolean usePrev) {
        Assert.neqNull(sourceDataIndex, "sourceDataIndex");
        Assert.gt(sourceKeyColumns.length, "sourceKeyColumns.length", 1);

        final WritableRowSet matching;
        try (final WritableRowSet possiblyMatching = RowSetFactory.empty()) {
            // First, compute a possibly-matching subset of selection based on the partial index.

            // noinspection DataFlowIssue
            final DataIndex.RowKeyLookup rowKeyLookup = sourceDataIndex.rowKeyLookup();
            final ColumnSource<RowSet> rowSetColumn = sourceDataIndex.rowSetColumn();

            final long kernelGeneration = sharedSet.beginRead();
            final Iterator<Object> values;
            final Function<Object, Object> keyMappingFunction;

            if (staticSetLookupKeys != null) {
                values = staticSetLookupKeys.iterator();
                keyMappingFunction = Function.identity();
            } else {
                values = sharedSet.kernel().iterator();
                if (sourceDataIndex.keyColumnNames().size() == 1) {
                    final int keyOffset = indexToTupleMap == null ? 0 : indexToTupleMap[0];
                    keyMappingFunction = (final Object key) -> sourceKeySource.exportElement(key, keyOffset);
                } else {
                    keyMappingFunction = tupleToPartialKeyMappingFunction();
                }
            }

            forEachKernelKey(values, kernelGeneration, key -> {
                final Object lookupKey = keyMappingFunction.apply(key);
                final long rowKey = rowKeyLookup.apply(lookupKey, usePrev);
                final RowSet rowSet = usePrev ? rowSetColumn.getPrev(rowKey) : rowSetColumn.get(rowKey);
                if (rowSet != null) {
                    try (final RowSet intersected = rowSet.intersect(selection)) {
                        possiblyMatching.insert(intersected);
                    }
                }
            });

            // Now, do linear filter on possiblyMatching to determine the values to include or exclude from selection.
            matching = filterLinear(possiblyMatching, true, usePrev);
        }
        if (inclusion) {
            return matching;
        }
        try (final SafeCloseable ignored = matching) {
            return selection.minus(matching);
        }
    }

    private WritableRowSet filterLinear(final RowSet selection, final boolean filterInclusion, final boolean usePrev) {
        if (selection.isEmpty()) {
            return RowSetFactory.empty();
        }

        final long kernelGeneration = sharedSet.beginRead();
        final SetInclusionKernel setKernel = sharedSet.kernel();

        final RowSetBuilderSequential filteredRowSetBuilder = RowSetFactory.builderSequential();

        final int maxChunkSize = getChunkSize(selection);
        // @formatter:off
        try (final ColumnSource.GetContext keyGetContext = sourceKeySource.makeGetContext(maxChunkSize);
             final RowSequence.Iterator selectionIterator = selection.getRowSequenceIterator();
             final WritableLongChunk<OrderedRowKeys> matchingKeys = WritableLongChunk.makeWritableChunk(maxChunkSize)) {
            // @formatter:on

            while (selectionIterator.hasMore()) {
                final RowSequence selectionChunk = selectionIterator.getNextRowSequenceWithLength(maxChunkSize);
                final LongChunk<OrderedRowKeys> selectionRowKeyChunk = selectionChunk.asRowKeyChunk();
                final Chunk<? extends Values> sourceChunk = usePrev
                        ? sourceKeySource.getPrevChunk(keyGetContext, selectionChunk)
                        : sourceKeySource.getChunk(keyGetContext, selectionChunk);
                final Chunk<Values> keyChunk = Chunk.downcast(sourceChunk);
                setKernel.matchValues(keyChunk, selectionRowKeyChunk, matchingKeys, filterInclusion);
                // A set change makes this attempt's results junk; abandon it rather than finish them.
                sharedSet.failIfChangedSince(kernelGeneration);
                filteredRowSetBuilder.appendOrderedRowKeysChunk(matchingKeys);
            }
        }

        return filteredRowSetBuilder.build();
    }

    private static int getChunkSize(@NotNull final RowSet selection) {
        return (int) Math.min(selection.size(), CHUNK_SIZE);
    }

    @Override
    public boolean isSimpleFilter() {
        /* This doesn't execute any user code, so it should be safe to execute it against untrusted data. */
        return true;
    }

    @Override
    public boolean isRefreshing() {
        return sharedSet.isRefreshing();
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {
        this.listener = listener;
        this.resultTable = listener.getTable();
        if (isRefreshing()) {
            listener.setIsRefreshing(true);
            // Only now can this filter act on a set change, so only now is it worth being told about one.
            sharedSet.addFilter(this);
        }
    }

    @Override
    protected void destroy() {
        super.destroy();
        // Stop the shared set from holding, and notifying, a filter whose result is gone.
        sharedSet.removeFilter(this);
    }

    /**
     * Called by {@link SharedSetKernel} when the shared keys change, so that this filter's result re-evaluates the rows
     * that may have changed status. Exclusion filters invert the requests.
     *
     * @param added Whether keys were added to the set
     * @param removed Whether keys were removed from the set
     */
    void onSetChanged(final boolean added, final boolean removed) {
        final RecomputeListener localListener = listener;
        if (localListener == null) {
            return;
        }
        if (added) {
            if (inclusion) {
                localListener.requestRecomputeUnmatched();
            } else {
                localListener.requestRecomputeMatched();
            }
        }
        if (removed) {
            if (inclusion) {
                localListener.requestRecomputeMatched();
            } else {
                localListener.requestRecomputeUnmatched();
            }
        }
    }

    /**
     * Called by {@link SharedSetKernel} when maintaining the shared keys fails, to fail this filter's result.
     */
    void onSetError(final Throwable originalException, final TableListener.Entry sourceEntry) {
        if (listener != null && resultTable != null) {
            resultTable.notifyListenersOnError(originalException, sourceEntry);
        }
    }

    @VisibleForTesting
    SharedSetKernel sharedSet() {
        return sharedSet;
    }

    @Override
    public DynamicWhereFilter copy() {
        return new DynamicWhereFilter(sharedSet, inclusion, sourceToSetColumnNamePairs);
    }

    @Override
    public boolean stateChangedOnStep(final long step) {
        return sharedSet.stateChangedOnStep(step);
    }

    @Override
    public boolean satisfied(final long step) {
        final boolean indexSatisfied = sourceDataIndex == null || sourceDataIndex.table().satisfied(step);
        return indexSatisfied && sharedSet.satisfied(step);
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("DynamicWhereFilter(")
                .append(MatchPair.MATCH_PAIR_ARRAY_FORMATTER, sourceToSetColumnNamePairs)
                .append(')');
    }

    @Override
    public LongStream parentPerformanceEntryIds() {
        return sharedSet.parentPerformanceEntryIds();
    }
}
