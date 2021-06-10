/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.SortPair;
import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.v2.hashing.HashMapK4V4;
import io.deephaven.db.v2.hashing.HashMapLockFreeK4V4;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ReadOnlyRedirectedColumnSource;
import io.deephaven.db.v2.sources.WritableChunkSink;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.utils.*;
import io.deephaven.util.SafeCloseableList;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class SortOperation implements QueryTable.MemoizableOperation<QueryTable> {

    private final QueryTable parent;
    private QueryTable resultTable;
    private RedirectionIndex sortMapping;

    private final SortPair[] sortPairs;
    private final SortingOrder[] sortOrder;
    private final String[] sortColumnNames;
    private final ColumnSource<Comparable<?>>[] sortColumns;

    public SortOperation(QueryTable parent, SortPair[] sortPairs) {
        this.parent = parent;
        this.sortPairs = sortPairs;
        this.sortOrder = Arrays.stream(sortPairs).map(SortPair::getOrder).toArray(SortingOrder[]::new);
        this.sortColumnNames = Arrays.stream(sortPairs).map(SortPair::getColumn).toArray(String[]::new);

        //noinspection unchecked
        sortColumns = new ColumnSource[sortColumnNames.length];

        for (int ii = 0; ii < sortColumnNames.length; ++ii) {
            //noinspection unchecked
            sortColumns[ii] = QueryTable.maybeTransformToPrimitive(parent.getColumnSource(sortColumnNames[ii]));

            Require.requirement(Comparable.class.isAssignableFrom(sortColumns[ii].getType()) || sortColumns[ii].getType().isPrimitive(),
                    "Comparable.class.isAssignableFrom(sortColumns[ii].getType()) || sortColumns[ii].getType().isPrimitive()",
                    sortColumnNames[ii],"sortColumnNames[ii]", sortColumns[ii].getType(),"sortColumns[ii].getType()");
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
    public ShiftAwareSwapListener newSwapListener(QueryTable queryTable) {
        return new ShiftAwareSwapListener(queryTable) {
            @Override
            public synchronized boolean end(long clockCycle) {
                final boolean success = super.end(clockCycle);
                if (success) {
                    QueryTable.startTrackingPrev(resultTable.getColumnSources());
                    sortMapping.startTrackingPrevValues();
                }
                return success;
            }
        };
    }

    private static boolean alreadySorted(final QueryTable parent, @NotNull final SortHelpers.SortMapping sortedKeys) {
        if (sortedKeys.size() == 0) {
            return true;
        }
        final ReadOnlyIndex.Iterator it = parent.getIndex().iterator();
        return sortedKeys.forEachLong(currentKey -> currentKey == it.nextLong());
    }

    @NotNull
    private QueryTable historicalSort(SortHelpers.SortMapping sortedKeys) {
        if (alreadySorted(this.parent, sortedKeys)) {
            setSorted(this.parent);
            return this.parent;
        }

        final RedirectionIndex sortMapping = sortedKeys.makeHistoricalRedirectionIndex();
        final Index resultIndex = Index.FACTORY.getFlatIndex(sortedKeys.size());

        final Map<String, ColumnSource<?>> resultMap = new LinkedHashMap<>();
        for (Map.Entry<String, ColumnSource> stringColumnSourceEntry : this.parent.getColumnSourceMap().entrySet()) {
            //noinspection unchecked
            resultMap.put(stringColumnSourceEntry.getKey(), new ReadOnlyRedirectedColumnSource<>(sortMapping, stringColumnSourceEntry.getValue()));
        }

        resultTable = new QueryTable(resultIndex, resultMap);
        this.parent.copyAttributes(resultTable, BaseTable.CopyAttributeOperation.Sort);
        resultTable.setFlat();
        setSorted(resultTable);
        return resultTable;
    }

    private void setSorted(QueryTable table) {
        // no matter what we are always sorted by the first column
        SortedColumnsAttribute.setOrderForColumn(table, sortColumnNames[0], sortOrder[0]);
    }

    @Override
    public Result initialize(boolean usePrev, long beforeClock) {
        if (!parent.isRefreshing()) {
            final SortHelpers.SortMapping sortedKeys = SortHelpers.getSortedKeys(sortOrder, sortColumns, parent.getIndex(), false);
            return new Result(historicalSort(sortedKeys));
        }

        try (final SafeCloseableList closer = new SafeCloseableList()) {
            // reset the sort data structures that we share between invocations
            final Map<String, ColumnSource<?>> resultMap = new LinkedHashMap<>();

            final Index indexToSort = usePrev ? closer.add(parent.getIndex().getPrevIndex()) : parent.getIndex();

            if (indexToSort.size() >= Integer.MAX_VALUE) {
                throw new UnsupportedOperationException("Can not perform ticking sort for table larger than " + Integer.MAX_VALUE + " rows, table is" + indexToSort.size());
            }

            final long[] sortedKeys = SortHelpers.getSortedKeys(sortOrder, sortColumns, indexToSort, usePrev).getArrayMapping();

            final HashMapK4V4 reverseLookup = new HashMapLockFreeK4V4(sortedKeys.length, .75f, -3);
            sortMapping = SortHelpers.createSortRedirectionIndex();

            // Center the keys around middleKeyToUse
            final long offset = SortListener.REBALANCE_MIDPOINT - sortedKeys.length / 2;
            final Index resultIndex = sortedKeys.length == 0 ? Index.FACTORY.getEmptyIndex() :
                    Index.FACTORY.getIndexByRange(offset, offset + sortedKeys.length - 1);

            for (int i = 0; i < sortedKeys.length; i++) {
                reverseLookup.put(sortedKeys[i], i + offset);
            }

            // fillFromChunk may convert the provided OrderedKeys to a KeyRanges (or KeyIndices) chunk that is owned by
            // the Index and is not closed until the index is closed.
            WritableChunkSink.FillFromContext fillFromContext = closer.add(sortMapping.makeFillFromContext(sortedKeys.length));
            sortMapping.fillFromChunk(fillFromContext, LongChunk.chunkWrap(sortedKeys), closer.add(resultIndex.clone()));

            for (Map.Entry<String, ColumnSource> stringColumnSourceEntry : parent.getColumnSourceMap().entrySet()) {
                //noinspection unchecked
                resultMap.put(stringColumnSourceEntry.getKey(), new ReadOnlyRedirectedColumnSource<>(sortMapping, stringColumnSourceEntry.getValue()));
            }

            //noinspection unchecked
            final ColumnSource<Comparable<?>>[] sortedColumnsToSortBy = Arrays.stream(sortColumnNames).map(resultMap::get).toArray(ColumnSource[]::new);
            // we also reinterpret our sortedColumnsToSortBy, which are guaranteed to be redirected sources of the inner source
            for (int ii = 0; ii < sortedColumnsToSortBy.length; ++ii) {
                //noinspection unchecked
                sortedColumnsToSortBy[ii] = QueryTable.maybeTransformToPrimitive(sortedColumnsToSortBy[ii]);
            }

            resultTable = new QueryTable(resultIndex, resultMap);
            parent.copyAttributes(resultTable, BaseTable.CopyAttributeOperation.Sort);

            final SortListener listener = new SortListener(parent, resultTable, reverseLookup, sortColumns, sortOrder,
                    sortMapping, sortedColumnsToSortBy, parent.newModifiedColumnSetIdentityTransformer(resultTable),
                    parent.newModifiedColumnSet(sortColumnNames));

            setSorted(resultTable);

            return new Result(resultTable, listener);
        }
    }
}
