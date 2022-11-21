/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.util.datastructures.hash.HashMapK4V4;
import io.deephaven.util.datastructures.hash.HashMapLockFreeK4V4;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.sources.SwitchColumnSource;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.LongChunkColumnSource;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.util.SafeCloseableList;

import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class SortOperation implements QueryTable.MemoizableOperation<QueryTable> {

    private final QueryTable parent;
    private QueryTable resultTable;
    private RowRedirection sortMapping;

    private final SortPair[] sortPairs;
    private final SortingOrder[] sortOrder;
    private final String[] sortColumnNames;
    private final ColumnSource<Comparable<?>>[] sortColumns;

    public SortOperation(QueryTable parent, SortPair[] sortPairs) {
        this.parent = parent;
        this.sortPairs = sortPairs;
        this.sortOrder = Arrays.stream(sortPairs).map(SortPair::getOrder).toArray(SortingOrder[]::new);
        this.sortColumnNames = Arrays.stream(sortPairs).map(SortPair::getColumn).toArray(String[]::new);

        // noinspection unchecked
        sortColumns = new ColumnSource[sortColumnNames.length];

        for (int ii = 0; ii < sortColumnNames.length; ++ii) {
            // noinspection unchecked
            sortColumns[ii] = (ColumnSource<Comparable<?>>) QueryTable
                    .maybeTransformToPrimitive(parent.getColumnSource(sortColumnNames[ii]));

            Require.requirement(
                    Comparable.class.isAssignableFrom(sortColumns[ii].getType())
                            || sortColumns[ii].getType().isPrimitive(),
                    "Comparable.class.isAssignableFrom(sortColumns[ii].getType()) || sortColumns[ii].getType().isPrimitive()",
                    sortColumnNames[ii], "sortColumnNames[ii]", sortColumns[ii].getType(), "sortColumns[ii].getType()");
        }

        parent.assertSortable(sortColumnNames);
    }

    @Override
    public String getDescription() {
        return "sort(" + Arrays.toString(sortPairs) + ")";
    }

    @Override
    public String getLogPrefix() {
        return "sort";
    }

    @Override
    public MemoizedOperationKey getMemoizedOperationKey() {
        return MemoizedOperationKey.sort(sortPairs);
    }

    @Override
    public SwapListener newSwapListener(QueryTable queryTable) {
        return new SwapListener(queryTable) {
            @Override
            public synchronized boolean end(long clockCycle) {
                final boolean success = super.end(clockCycle);
                if (success) {
                    QueryTable.startTrackingPrev(resultTable.getColumnSources());
                    if (sortMapping.isWritable()) {
                        sortMapping.writableCast().startTrackingPrevValues();
                    }
                }
                return success;
            }
        };
    }

    private static boolean alreadySorted(final QueryTable parent, @NotNull final SortHelpers.SortMapping sortedKeys) {
        if (sortedKeys.size() == 0) {
            return true;
        }
        final RowSet.Iterator it = parent.getRowSet().iterator();
        return sortedKeys.forEachLong(currentKey -> currentKey == it.nextLong());
    }

    @NotNull
    private QueryTable historicalSort(SortHelpers.SortMapping sortedKeys) {
        if (alreadySorted(this.parent, sortedKeys)) {
            setSorted(this.parent);
            return this.parent;
        }

        final WritableRowRedirection sortMapping = sortedKeys.makeHistoricalRowRedirection();
        final TrackingRowSet resultRowSet = RowSetFactory.flat(sortedKeys.size()).toTracking();

        final Map<String, ColumnSource<?>> resultMap = new LinkedHashMap<>();
        for (Map.Entry<String, ColumnSource<?>> stringColumnSourceEntry : this.parent.getColumnSourceMap().entrySet()) {
            resultMap.put(stringColumnSourceEntry.getKey(),
                    new RedirectedColumnSource<>(sortMapping, stringColumnSourceEntry.getValue()));
        }

        resultTable = new QueryTable(resultRowSet, resultMap);
        this.parent.copyAttributes(resultTable, BaseTable.CopyAttributeOperation.Sort);
        resultTable.setFlat();
        setSorted(resultTable);
        return resultTable;
    }

    @NotNull
    private Result<QueryTable> streamSort(@NotNull final SortHelpers.SortMapping initialSortedKeys) {
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
                    new RedirectedColumnSource<>(sortMapping, stringColumnSourceEntry.getValue()));
        }

        resultTable = new QueryTable(resultRowSet, resultMap);
        parent.copyAttributes(resultTable, BaseTable.CopyAttributeOperation.Sort);
        resultTable.setFlat();
        setSorted(resultTable);

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
                                SortHelpers.getSortedKeys(sortOrder, sortColumns, upstream.added(), false);
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

    @Override
    public Result<QueryTable> initialize(boolean usePrev, long beforeClock) {
        if (!parent.isRefreshing()) {
            final SortHelpers.SortMapping sortedKeys =
                    SortHelpers.getSortedKeys(sortOrder, sortColumns, parent.getRowSet(), false);
            return new Result<>(historicalSort(sortedKeys));
        }
        if (parent.isStream()) {
            try (final RowSet prevIndex = usePrev ? parent.getRowSet().copyPrev() : null) {
                final RowSet indexToUse = usePrev ? prevIndex : parent.getRowSet();
                final SortHelpers.SortMapping sortedKeys =
                        SortHelpers.getSortedKeys(sortOrder, sortColumns, indexToUse, usePrev);
                return streamSort(sortedKeys);
            }
        }

        try (final SafeCloseableList closer = new SafeCloseableList()) {
            // reset the sort data structures that we share between invocations
            final Map<String, ColumnSource<?>> resultMap = new LinkedHashMap<>();

            final RowSet rowSetToSort = usePrev ? closer.add(parent.getRowSet().copyPrev()) : parent.getRowSet();

            if (rowSetToSort.size() >= Integer.MAX_VALUE) {
                throw new UnsupportedOperationException("Can not perform ticking sort for table larger than "
                        + Integer.MAX_VALUE + " rows, table is" + rowSetToSort.size());
            }

            final long[] sortedKeys =
                    SortHelpers.getSortedKeys(sortOrder, sortColumns, rowSetToSort, usePrev).getArrayMapping();

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

            for (Map.Entry<String, ColumnSource<?>> stringColumnSourceEntry : parent.getColumnSourceMap().entrySet()) {
                resultMap.put(stringColumnSourceEntry.getKey(),
                        new RedirectedColumnSource<>(sortMapping, stringColumnSourceEntry.getValue()));
            }

            // noinspection unchecked
            final ColumnSource<Comparable<?>>[] sortedColumnsToSortBy =
                    Arrays.stream(sortColumnNames).map(resultMap::get).toArray(ColumnSource[]::new);
            // we also reinterpret our sortedColumnsToSortBy, which are guaranteed to be redirected sources of the inner
            // source
            for (int ii = 0; ii < sortedColumnsToSortBy.length; ++ii) {
                // noinspection unchecked
                sortedColumnsToSortBy[ii] =
                        (ColumnSource<Comparable<?>>) QueryTable.maybeTransformToPrimitive(sortedColumnsToSortBy[ii]);
            }

            resultTable = new QueryTable(resultRowSet, resultMap);
            parent.copyAttributes(resultTable, BaseTable.CopyAttributeOperation.Sort);

            final SortListener listener = new SortListener(parent, resultTable, reverseLookup, sortColumns, sortOrder,
                    sortMapping.writableCast(), sortedColumnsToSortBy,
                    parent.newModifiedColumnSetIdentityTransformer(resultTable),
                    parent.newModifiedColumnSet(sortColumnNames));

            setSorted(resultTable);

            return new Result<>(resultTable, listener);
        }
    }

    public static RowRedirection getRowRedirection(@NotNull final Table sortResult) {
        final String firstColumnName = sortResult.getDefinition().getColumns().get(0).getName();
        final ColumnSource<?> firstColumnSource = sortResult.getColumnSource(firstColumnName);
        if (!(firstColumnSource instanceof RedirectedColumnSource)) {
            throw new IllegalArgumentException("Sort result's first column is not redirected");
        }
        return ((RedirectedColumnSource) firstColumnSource).getRowRedirection();
    }
}
