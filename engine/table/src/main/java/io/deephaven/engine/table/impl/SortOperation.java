//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.SortSpec;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.SwitchColumnSource;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.LongChunkColumnSource;
import io.deephaven.engine.table.impl.util.LongColumnSourceRowRedirection;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.iterators.ChunkedLongColumnIterator;
import io.deephaven.engine.table.iterators.LongColumnIterator;
import io.deephaven.util.SafeCloseableList;
import io.deephaven.util.datastructures.hash.HashMapK4V4;
import io.deephaven.util.datastructures.hash.HashMapLockFreeK4V4;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.LongUnaryOperator;

import static io.deephaven.engine.table.Table.SORT_REVERSE_LOOKUP_ATTRIBUTE;
import static io.deephaven.engine.table.Table.SORT_ROW_REDIRECTION_ATTRIBUTE;
import static io.deephaven.engine.table.impl.SortHelpers.AllowSymbolTable.ALLOW_SYMBOL_TABLE;
import static io.deephaven.engine.table.impl.SortHelpers.AllowSymbolTable.DISALLOW_SYMBOL_TABLE;

public class SortOperation implements QueryTable.MemoizableOperation<QueryTable> {
    static final Map<String, Object> IDENTITY_REDIRECTION_ATTRIBUTES;
    // The "+" sign is not valid in a column, therefore we can be sure that this is a proper sentinel value.
    static final String IDENTITY_REDIRECTION_VALUE = "+IDENTITY_REDIRECTION";
    static {
        final HashMap<String, Object> identityRedirectionAttributes = new HashMap<>();
        identityRedirectionAttributes.put(SORT_ROW_REDIRECTION_ATTRIBUTE, IDENTITY_REDIRECTION_VALUE);
        IDENTITY_REDIRECTION_ATTRIBUTES = Collections.unmodifiableMap(identityRedirectionAttributes);
    }

    private final QueryTable parent;
    private QueryTable resultTable;
    private RowRedirection sortMapping;

    private final String[] sortColumnNames;
    private final SortSpec[] sortSpec;
    private final SortingOrder[] sortOrder;
    private final Comparator[] comparators;
    private final boolean[] comparatorsRespectEquality;
    /** Stores original column sources. */
    private final ColumnSource<Comparable<?>>[] originalSortColumns;
    /** Stores reinterpreted column sources. */
    private final ColumnSource<Comparable<?>>[] sortColumns;

    private final DataIndex dataIndex;

    public SortOperation(final QueryTable parent, final SortSpec[] sortSpecIn) {
        this.parent = parent;
        this.sortSpec = sortSpecIn;
        this.sortOrder = Arrays.stream(sortSpecIn).map(SortingOrder::from).toArray(SortingOrder[]::new);
        this.sortColumnNames = new String[sortSpecIn.length];
        this.comparators = new Comparator[sortSpec.length];
        this.comparatorsRespectEquality = new boolean[sortSpec.length];
        // noinspection unchecked
        originalSortColumns = new ColumnSource[sortSpecIn.length];
        // noinspection unchecked
        sortColumns = new ColumnSource[sortSpecIn.length];

        for (int ii = 0; ii < sortSpecIn.length; ++ii) {
            final String sortColumnName = sortColumnNames[ii] = sortSpecIn[ii].column().name();

            originalSortColumns[ii] = parent.getColumnSource(sortColumnName);
            // noinspection unchecked
            sortColumns[ii] =
                    (ColumnSource<Comparable<?>>) ReinterpretUtils.maybeConvertToPrimitive(originalSortColumns[ii]);

            final Class<?> columnType = sortColumns[ii].getType();

            if (ComparatorSortColumn.hasComparator(sortSpecIn[ii])) {
                if (columnType.isPrimitive()) {
                    throw new NotSortableColumnException(sortColumnName + " is a primitive column (" + columnType
                            + "), therefore cannot accept a Comparator");
                }

                comparators[ii] = ((ComparatorSortColumn) sortSpecIn[ii]).comparator();
                comparatorsRespectEquality[ii] = ((ComparatorSortColumn) sortSpecIn[ii]).respectsEquality();
            } else {
                comparatorsRespectEquality[ii] = true;

                final Comparator<?> defaultComparator = ComparatorRegistry.INSTANCE.getComparator(columnType);
                if (defaultComparator == null) {
                    final boolean isSortable =
                            Comparable.class.isAssignableFrom(columnType) || columnType.isPrimitive();
                    if (!isSortable) {
                        throw new NotSortableColumnException(sortColumnName + " is not a sortable type: " + columnType);
                    }
                } else {
                    comparators[ii] = defaultComparator;
                }
            }
        }

        parent.assertSortable(sortColumnNames);

        // This sort operation might leverage a data index.
        dataIndex = optimalIndex(parent);
    }

    @Override
    public String getDescription() {
        return "sort(" + Arrays.toString(sortSpec) + ")";
    }

    @Override
    public String getLogPrefix() {
        return "sort";
    }

    @Override
    public MemoizedOperationKey getMemoizedOperationKey() {
        return MemoizedOperationKey.sort(sortSpec);
    }

    @Override
    public OperationSnapshotControl newSnapshotControl(QueryTable queryTable) {
        return dataIndex != null
                ? new OperationSnapshotControlEx(queryTable, dataIndex.table())
                : new OperationSnapshotControl(queryTable);
    }

    /**
     * Returns the optimal data index for the supplied table, or null if no index is available. The ideal index would
     * contain all key columns but matching the first column is still useful.
     */
    @Nullable
    private DataIndex optimalIndex(final Table inputTable) {
        final DataIndex full = DataIndexer.getDataIndex(inputTable, sortColumnNames);
        if (full != null) {
            // We have an index for all sort columns.
            return full;
        }
        // Return an index for the first column (if one exists) or null.
        return DataIndexer.getDataIndex(inputTable, sortColumnNames[0]);
    }

    private static boolean alreadySorted(final QueryTable parent, @NotNull final SortHelpers.SortMapping sortedKeys) {
        if (sortedKeys.size() == 0) {
            return true;
        }
        try (RowSet.Iterator it = parent.getRowSet().iterator()) {
            return sortedKeys.forEachLong(currentKey -> currentKey == it.nextLong());
        }
    }

    @NotNull
    private QueryTable historicalSort(SortHelpers.SortMapping sortedKeys) {
        if (alreadySorted(parent, sortedKeys)) {
            return withSorted(parent);
        }

        final WritableRowRedirection sortMapping = sortedKeys.makeHistoricalRowRedirection();
        final TrackingRowSet resultRowSet = RowSetFactory.flat(sortedKeys.size()).toTracking();

        final Map<String, ColumnSource<?>> resultMap = new LinkedHashMap<>();
        final String sortMappingColumnName = populateRedirectedColumns(resultMap, sortMapping);

        resultTable = new QueryTable(resultRowSet, resultMap);
        parent.copyAttributes(resultTable, BaseTable.CopyAttributeOperation.Sort);
        resultTable.setFlat();
        resultTable.setAttribute(SORT_ROW_REDIRECTION_ATTRIBUTE, sortMappingColumnName);
        setSorted(resultTable);
        return resultTable;
    }

    @NotNull
    private Result<QueryTable> blinkTableSort(@NotNull final SortHelpers.SortMapping initialSortedKeys) {
        final LongChunkColumnSource initialInnerRedirectionSource = new LongChunkColumnSource();
        if (initialSortedKeys.size() > 0) {
            initialInnerRedirectionSource
                    .addChunk(WritableLongChunk.writableChunkWrap(initialSortedKeys.getArrayMapping()));
        }
        final MutableObject<LongChunkColumnSource> recycledInnerRedirectionSource = new MutableObject<>();
        final SwitchColumnSource<Long> redirectionSource = new SwitchColumnSource<>(initialInnerRedirectionSource,
                (final ColumnSource<Long> previousInnerRedirectionSource) -> {
                    final LongChunkColumnSource recycled = (LongChunkColumnSource) previousInnerRedirectionSource;
                    recycled.clear();
                    recycledInnerRedirectionSource.setValue(recycled);
                });

        sortMapping = new LongColumnSourceRowRedirection<>(redirectionSource);
        final TrackingWritableRowSet resultRowSet =
                RowSetFactory.flat(initialSortedKeys.size()).toTracking();

        final Map<String, ColumnSource<?>> resultMap = new LinkedHashMap<>();
        for (Map.Entry<String, ColumnSource<?>> stringColumnSourceEntry : parent.getColumnSourceMap().entrySet()) {
            resultMap.put(stringColumnSourceEntry.getKey(),
                    RedirectedColumnSource.maybeRedirect(sortMapping, stringColumnSourceEntry.getValue()));
        }

        resultTable = new QueryTable(resultRowSet, resultMap);
        parent.copyAttributes(resultTable, BaseTable.CopyAttributeOperation.Sort);
        resultTable.setFlat();
        setSorted(resultTable);

        QueryTable.startTrackingPrev(resultTable.getColumnSources());
        if (sortMapping.isWritable()) {
            sortMapping.writableCast().startTrackingPrevValues();
        }

        final TableUpdateListener resultListener =
                new BaseTable.ListenerImpl("Stream sort listener", parent, resultTable) {
                    @Override
                    public void onUpdate(@NotNull final TableUpdate upstream) {
                        Assert.assertion(upstream.modified().isEmpty() && upstream.shifted().empty(),
                                "upstream.modified.empty() && upstream.shifted.empty()");
                        Assert.eq(resultRowSet.size(), "resultRowSet.size()", upstream.removed().size(),
                                "upstream.removed.size()");
                        if (upstream.empty()) {
                            return;
                        }

                        final SortHelpers.SortMapping updateSortedKeys =
                                SortHelpers.getSortedKeys(sortOrder, originalSortColumns, sortColumns,
                                        comparators,
                                        comparatorsRespectEquality, null,
                                        upstream.added(), false, DISALLOW_SYMBOL_TABLE);
                        final LongChunkColumnSource recycled = recycledInnerRedirectionSource.getValue();
                        recycledInnerRedirectionSource.setValue(null);
                        final LongChunkColumnSource updateInnerRedirectSource =
                                recycled == null ? new LongChunkColumnSource() : recycled;
                        if (updateSortedKeys.size() > 0) {
                            updateInnerRedirectSource
                                    .addChunk(WritableLongChunk.writableChunkWrap(updateSortedKeys.getArrayMapping()));
                        }
                        redirectionSource.setNewCurrent(updateInnerRedirectSource);

                        final RowSet added = RowSetFactory.flat(upstream.added().size());
                        final RowSet removed = RowSetFactory.flat(upstream.removed().size());
                        if (added.size() > removed.size()) {
                            resultRowSet.insertRange(removed.size(), added.size() - 1);
                        } else if (removed.size() > added.size()) {
                            resultRowSet.removeRange(added.size(), removed.size() - 1);
                        }
                        resultTable.notifyListeners(new TableUpdateImpl(added, removed, RowSetFactory.empty(),
                                RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
                    }
                };

        return new Result<>(resultTable, resultListener);
    }

    private void setSorted(QueryTable table) {
        // no matter what we are always sorted by the first column
        SortedColumnsAttribute.setOrderForColumn(table, sortColumnNames[0], sortOrder[0]);
    }

    private QueryTable withSorted(QueryTable table) {
        return (QueryTable) SortedColumnsAttribute.withOrderForColumn(table, sortColumnNames[0], sortOrder[0],
                IDENTITY_REDIRECTION_ATTRIBUTES);
    }

    @Override
    public Result<QueryTable> initialize(boolean usePrev, long beforeClock) {
        if (!parent.isRefreshing()) {
            final SortHelpers.SortMapping sortedKeys =
                    SortHelpers.getSortedKeys(sortOrder, originalSortColumns, sortColumns, comparators,
                            comparatorsRespectEquality, dataIndex, parent.getRowSet(), false, ALLOW_SYMBOL_TABLE);
            return new Result<>(historicalSort(sortedKeys));
        }
        if (parent.isBlink()) {
            final RowSet rowSetToUse = usePrev ? parent.getRowSet().prev() : parent.getRowSet();
            final SortHelpers.SortMapping sortedKeys =
                    SortHelpers.getSortedKeys(sortOrder, originalSortColumns, sortColumns, comparators,
                            comparatorsRespectEquality, dataIndex, rowSetToUse, usePrev, ALLOW_SYMBOL_TABLE);
            return blinkTableSort(sortedKeys);
        }

        try (final SafeCloseableList closer = new SafeCloseableList()) {
            // reset the sort data structures that we share between invocations
            final Map<String, ColumnSource<?>> resultMap = new LinkedHashMap<>();

            final RowSet rowSetToSort = usePrev ? parent.getRowSet().prev() : parent.getRowSet();

            if (rowSetToSort.size() >= Integer.MAX_VALUE) {
                throw new UnsupportedOperationException("Can not perform ticking sort for table larger than "
                        + Integer.MAX_VALUE + " rows, table is" + rowSetToSort.size());
            }

            final long[] sortedKeys = SortHelpers
                    .getSortedKeys(sortOrder, originalSortColumns, sortColumns, comparators, comparatorsRespectEquality,
                            dataIndex, rowSetToSort, usePrev, ALLOW_SYMBOL_TABLE)
                    .getArrayMapping();

            final HashMapK4V4 reverseLookup = new HashMapLockFreeK4V4(sortedKeys.length, .75f, -3);
            sortMapping = SortHelpers.createSortRowRedirection();

            // Center the keys around middleKeyToUse
            final long offset = SortListener.REBALANCE_MIDPOINT - sortedKeys.length / 2;
            final TrackingRowSet resultRowSet = (sortedKeys.length == 0
                    ? RowSetFactory.empty()
                    : RowSetFactory.fromRange(offset, offset + sortedKeys.length - 1)).toTracking();

            for (int i = 0; i < sortedKeys.length; i++) {
                reverseLookup.put(sortedKeys[i], i + offset);
            }

            // fillFromChunk may convert the provided RowSequence to a KeyRanges (or RowKeys) chunk that is owned by
            // the RowSequence and is not closed until the RowSequence is closed.
            ChunkSink.FillFromContext fillFromContext =
                    closer.add(sortMapping.writableCast().makeFillFromContext(sortedKeys.length));
            sortMapping.writableCast().fillFromChunk(fillFromContext, LongChunk.chunkWrap(sortedKeys),
                    closer.add(resultRowSet.copy()));

            String sortMappingColumnName = populateRedirectedColumns(resultMap, sortMapping);

            // noinspection unchecked
            final ColumnSource<Comparable<?>>[] sortedColumnsToSortBy =
                    Arrays.stream(sortColumnNames).map(resultMap::get).toArray(ColumnSource[]::new);
            // we also reinterpret our sortedColumnsToSortBy, which are guaranteed to be redirected sources of the inner
            // source
            for (int ii = 0; ii < sortedColumnsToSortBy.length; ++ii) {
                // noinspection unchecked
                sortedColumnsToSortBy[ii] = (ColumnSource<Comparable<?>>) ReinterpretUtils
                        .maybeConvertToPrimitive(sortedColumnsToSortBy[ii]);
            }

            resultTable = new QueryTable(resultRowSet, resultMap);
            parent.copyAttributes(resultTable, BaseTable.CopyAttributeOperation.Sort);
            resultTable.setAttribute(SORT_ROW_REDIRECTION_ATTRIBUTE, sortMappingColumnName);
            setReverseLookup(resultTable, (final long innerRowKey) -> {
                final long outerRowKey = reverseLookup.get(innerRowKey);
                return outerRowKey == reverseLookup.getNoEntryValue() ? RowSequence.NULL_ROW_KEY : outerRowKey;
            });

            final SortListener listener = new SortListener(parent, resultTable, reverseLookup,
                    originalSortColumns, sortColumns, sortOrder, comparators,
                    comparatorsRespectEquality,
                    sortMapping.writableCast(), sortedColumnsToSortBy,
                    parent.newModifiedColumnSetIdentityTransformer(resultTable),
                    parent.newModifiedColumnSet(sortColumnNames));

            setSorted(resultTable);

            QueryTable.startTrackingPrev(resultTable.getColumnSources());
            if (sortMapping.isWritable()) {
                sortMapping.writableCast().startTrackingPrevValues();
            }

            return new Result<>(resultTable, listener);
        }
    }

    private String populateRedirectedColumns(Map<String, ColumnSource<?>> resultMap, RowRedirection sortMapping) {
        // if nothing is actually redirected, we can use the identity value
        String sortMappingColumnName = IDENTITY_REDIRECTION_VALUE;

        for (Map.Entry<String, ColumnSource<?>> stringColumnSourceEntry : parent.getColumnSourceMap().entrySet()) {
            final ColumnSource<?> innerSource = stringColumnSourceEntry.getValue();
            final ColumnSource<?> redirectedSource = RedirectedColumnSource.maybeRedirect(sortMapping, innerSource);
            resultMap.put(stringColumnSourceEntry.getKey(), redirectedSource);
            if (redirectedSource != innerSource) {
                sortMappingColumnName = stringColumnSourceEntry.getKey();
            }
        }
        return sortMappingColumnName;
    }

    /**
     * Get the row redirection for a sort result.
     *
     * @param sortResult The sort result table; <em>must</em> be the direct result of a sort.
     * @return The row redirection for this table if at least one column required redirection, otherwise {@code null}
     */
    public static RowRedirection getRowRedirection(@NotNull final Table sortResult) {
        final String columnName = (String) sortResult.getAttribute(SORT_ROW_REDIRECTION_ATTRIBUTE);
        if (columnName == null || columnName.equals(IDENTITY_REDIRECTION_VALUE)) {
            return null;
        }

        return ((RedirectedColumnSource<?>) sortResult.getColumnSource(columnName)).getRowRedirection();
    }

    /**
     * Get a reverse lookup for a sort result, providing a mapping from input row key (that is, in the parent table's
     * row key space) to output row key (that is, in the sorted table's row space). Unknown input row keys are mapped to
     * the {@link RowSequence#NULL_ROW_KEY null row key}. This is effectively the reverse of the mapping provided by the
     * sort's {@link RowRedirection}.
     * <p>
     * Unsupported if the sort result's parent was a {@link BaseTable#isBlink() blink table}.
     * <p>
     * For refreshing tables, using the reverse lookup concurrently requires careful consideration. The mappings are
     * always against "current" data. It is only safe to use before the parent table notifies on a given cycle, or after
     * the sorted table notifies (and during idle phases).
     * <p>
     * For static tables, do note that the reverse lookup will be produced on-demand within this method.
     *
     * @param parent The sort input table; must have been sorted in order to produce {@code sortResult}
     * @param sortResult The sort result table; <em>must</em> be the direct result of a sort on {@code parent}
     * @return The reverse lookup, or null if no redirection is performed.
     */
    public static LongUnaryOperator getReverseLookup(@NotNull final Table parent, @NotNull final Table sortResult) {
        if (BlinkTableTools.isBlink(parent)) {
            throw new UnsupportedOperationException("Blink tables do not support sort reverse lookup");
        }
        final Object value = sortResult.getAttribute(SORT_REVERSE_LOOKUP_ATTRIBUTE);
        if (sortResult.isRefreshing()) {
            Assert.neqNull(value, "sort result reverse lookup");
        }
        if (value != null) {
            return (LongUnaryOperator) value;
        }
        final RowRedirection sortRedirection = getRowRedirection(sortResult);
        if (sortRedirection == null) {
            return null;
        }
        final HashMapK4V4 reverseLookup = new HashMapLockFreeK4V4(sortResult.intSize(), .75f, RowSequence.NULL_ROW_KEY);
        try (final LongColumnIterator innerRowKeys =
                new ChunkedLongColumnIterator(sortRedirection, sortResult.getRowSet());
                final RowSet.Iterator outerRowKeys = sortResult.getRowSet().iterator()) {
            while (outerRowKeys.hasNext()) {
                reverseLookup.put(innerRowKeys.nextLong(), outerRowKeys.nextLong());
            }
        }
        return reverseLookup::get;
    }

    private static void setReverseLookup(
            @NotNull final QueryTable sortResult,
            @NotNull final LongUnaryOperator reverseLookup) {
        sortResult.setAttribute(SORT_REVERSE_LOOKUP_ATTRIBUTE, reverseLookup);
    }
}
