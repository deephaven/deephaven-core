//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.ExposesChunkFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.deephaven.engine.util.NullSafeAddition.plusLong;
import static io.deephaven.engine.util.TableTools.emptyTable;

public class CountWhereOperator implements IterativeChunkedAggregationOperator {
    private final String resultName;
    final LongArraySource resultColumnSource;
    // Only AND or OR filters are populated in these arrays
    final boolean isAnd;
    final WhereFilter[] whereFilters;
    final ExposesChunkFilter[] chunkedWhereFilters;

    /**
     * A table constructed from chunk sources, will be populated with incoming chunk data and used to evaluate filters.
     */
    final Table chunkSourceTable;
    final ChunkColumnSource<?> chunkColumnSource;

    static class CountWhereBucketedContext implements BucketedContext {
        final WritableLongChunk<OrderedRowKeys> fullKeyChunk;
        final WritableLongChunk<OrderedRowKeys> workingKeyChunk;
        final WritableLongChunk<OrderedRowKeys> outputKeyChunk;
        final WritableChunk<Values> workingValueChunk;
        final WritableIntChunk<Values> countChunk;

        public CountWhereBucketedContext(final int size, final ChunkType chunkType) {
            fullKeyChunk = WritableLongChunk.makeWritableChunk(size);
            for (int ii = 0; ii < size; ++ii) {
                fullKeyChunk.set(ii, ii);
            }
            workingKeyChunk = WritableLongChunk.makeWritableChunk(size);
            outputKeyChunk = WritableLongChunk.makeWritableChunk(size);
            workingValueChunk = chunkType == null ? null : chunkType.makeWritableChunk(size);
            countChunk = WritableIntChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            fullKeyChunk.close();
            workingKeyChunk.close();
            outputKeyChunk.close();
            if (workingValueChunk != null) {
                workingValueChunk.close();
            }
            countChunk.close();
        }
    }

    static class CountWhereSingletonContext implements SingletonContext {
        final WritableLongChunk<OrderedRowKeys> fullKeyChunk;
        final WritableLongChunk<OrderedRowKeys> workingKeyChunk;
        final WritableLongChunk<OrderedRowKeys> outputKeyChunk;
        final WritableChunk<Values> workingValueChunk;

        public CountWhereSingletonContext(final int size, final ChunkType chunkType) {
            fullKeyChunk = WritableLongChunk.makeWritableChunk(size);
            for (int ii = 0; ii < size; ++ii) {
                fullKeyChunk.set(ii, ii);
            }
            workingKeyChunk = WritableLongChunk.makeWritableChunk(size);
            outputKeyChunk = WritableLongChunk.makeWritableChunk(size);
            workingValueChunk = chunkType == null ? null : chunkType.makeWritableChunk(size);
        }

        @Override
        public void close() {
            fullKeyChunk.close();
            workingKeyChunk.close();
            outputKeyChunk.close();
            if (workingValueChunk != null) {
                workingValueChunk.close();
            }
        }
    }

    /**
     * Construct a count aggregation operator that tests individual data values.
     *
     * @param resultName The name of the result column
     */
    CountWhereOperator(
            @NotNull final String resultName,
            final WhereFilter[] whereFilters,
            @Nullable final String inputColumnName,
            @Nullable final ColumnSource<?> inputSource) {
        this.resultName = resultName;
        this.resultColumnSource = new LongArraySource();

        if (whereFilters.length == 1 && whereFilters[0] instanceof DisjunctiveFilter) {
            final List<WhereFilter> orFilters = ((DisjunctiveFilter) whereFilters[0]).getFilters();
            this.whereFilters = orFilters.stream()
                    .filter(filter -> !(filter instanceof ExposesChunkFilter))
                    .toArray(WhereFilter[]::new);
            chunkedWhereFilters = orFilters.stream()
                    .filter(ExposesChunkFilter.class::isInstance)
                    .map(ExposesChunkFilter.class::cast)
                    .toArray(ExposesChunkFilter[]::new);
            isAnd = false;
        } else {
            this.whereFilters = Arrays.stream(whereFilters)
                    .filter(filter -> !(filter instanceof ExposesChunkFilter))
                    .toArray(WhereFilter[]::new);
            chunkedWhereFilters = Arrays.stream(whereFilters)
                    .filter(ExposesChunkFilter.class::isInstance)
                    .map(ExposesChunkFilter.class::cast)
                    .toArray(ExposesChunkFilter[]::new);
            isAnd = true;
        }

        if (inputColumnName == null || inputSource == null) {
            chunkColumnSource = null;
            chunkSourceTable = emptyTable(0);
        } else {
            chunkColumnSource = ChunkColumnSource.make(inputSource.getChunkType(), inputSource.getType(),
                    inputSource.getComponentType());
            chunkSourceTable = new QueryTable(RowSetFactory.empty().toTracking(),
                    Collections.singletonMap(inputColumnName, chunkColumnSource));
        }
    }

    private int doCount(int chunkStart, int chunkSize, Chunk<? extends Values> values) {
        if (chunkColumnSource != null) {
            // Clear the chunk sources and add the new sliced chunk
            chunkColumnSource.clear();
            // TODO: Can this cast ever fail? Investigate.
            final WritableChunk<? extends Values> slicedChunk =
                    (WritableChunk<? extends Values>) values.slice(chunkStart, chunkSize);
            chunkColumnSource.addChunk(slicedChunk);
        }

        // NOTE: we don't need to modify the table RowSet since we are supplying the filters directly
        try (final RowSet fullRowSet = RowSetFactory.flat(chunkSize)) {
            final Mutable<RowSet> result = new MutableObject<>(RowSetFactory.flat(chunkSize));
            // Apply the filters successively to the chunk source table
            for (WhereFilter whereFilter : whereFilters) {
                try (final RowSet current = result.getValue()) {
                    result.setValue(whereFilter.filter(current, fullRowSet, chunkSourceTable, false));
                }
            }
            try (final RowSet filtered = result.getValue()) {
                return filtered.intSize();
            }
        }
    }

    private static void chunkFilterAnd(
            final ExposesChunkFilter[] filters,
            final Chunk<? extends Values> sourceValues,
            final WritableLongChunk<OrderedRowKeys> sourceKeys,
            final WritableChunk<Values> workingValues,
            final WritableLongChunk<OrderedRowKeys> finalKeys) {
        // Apply the first filter
        filters[0].chunkFilter().filter(sourceValues, sourceKeys, finalKeys);
        Chunk<? extends Values> sourceValueRef = sourceValues;

        for (int filterIndex = 1; filterIndex < filters.length; filterIndex++) {
            if (sourceValueRef != null && finalKeys.size() < sourceValueRef.size()) {
                // Condense the values for the next pass
                for (int ii = 0; ii < finalKeys.size(); ii++) {
                    final int key = (int) finalKeys.get(ii);
                    workingValues.copyFromChunk(sourceValueRef, key, ii, 1);
                }
                sourceValueRef = workingValues;
            }
            // Do the next round of filtering
            filters[filterIndex].chunkFilter().filter(sourceValueRef, finalKeys, finalKeys);
        }
    }

    private static void mergeSortedChunks(
            final WritableLongChunk<OrderedRowKeys> first,
            final WritableLongChunk<OrderedRowKeys> second) {
        if (second.size() == 0) {
            return;
        }
        if (first.size() == 0) {
            first.copyFromChunk(second, 0, 0, second.size());
            first.setSize(second.size());
            return;
        }

        int firstIndex = first.size() - 1;
        int secondIndex = second.size() - 1;
        int destIndex = first.size() + second.size() - 1;

        while (firstIndex >= 0 && secondIndex >= 0) {
            final long firstValue = first.get(firstIndex);
            final long secondValue = second.get(secondIndex);
            if (firstValue > secondValue) {
                first.set(destIndex, firstValue);
                firstIndex--;
            } else {
                first.set(destIndex, secondValue);
                secondIndex--;
            }
            destIndex--;
        }
        while (secondIndex >= 0) {
            first.set(destIndex, second.get(secondIndex));
            secondIndex--;
            destIndex--;
        }
        // first index items are already in place
        first.setSize(first.size() + second.size());
    }

    private static void chunkFilterOr(
            final ExposesChunkFilter[] filters,
            final Chunk<? extends Values> sourceValues,
            final WritableLongChunk<OrderedRowKeys> sourceKeys,
            final WritableLongChunk<OrderedRowKeys> workingKeys,
            final WritableChunk<Values> workingValues,
            final WritableLongChunk<OrderedRowKeys> finalKeys) {
        // Apply the first filter
        filters[0].chunkFilter().filter(sourceValues, sourceKeys, finalKeys);

        Chunk<? extends Values> sourceValueRef = sourceValues;
        WritableLongChunk<OrderedRowKeys> sourceKeysRef = sourceKeys;

        // We need to OR the results of the succeeding filters
        for (int filterIndex = 1; filterIndex < filters.length; filterIndex++) {
            if (sourceValueRef != null && finalKeys.size() < sourceValueRef.size()) {
                // Condense the values for the next pass by including only the values that are not already in finalKeys
                int workingKey = 0;
                int workingKeyIndex = 0;
                workingKeys.setSize(0);
                for (int ii = 0; ii < finalKeys.size(); ii++) {
                    final int key = (int) finalKeys.get(ii);
                    while (workingKey < key) {
                        workingKeys.add(workingKey);
                        workingValues.copyFromChunk(sourceValues, workingKey, workingKeyIndex, 1);
                        workingKey++;
                        workingKeyIndex++;
                    }
                    workingKey = key + 1;
                }
                sourceKeysRef = workingKeys;
                sourceValueRef = workingValues;
                workingValues.setSize(workingKeys.size());
            }
            // Do the next round of filtering
            filters[filterIndex].chunkFilter().filter(sourceValueRef, sourceKeysRef, workingKeys);
            mergeSortedChunks(finalKeys, workingKeys);
        }
    }


    /**
     * Given the data chunk and multiple destinations, count the number of rows that pass the filters and store into the
     * count chunk.
     */
    private void doCountMultiple(
            Chunk<? extends Values> values,
            IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions,
            IntChunk<ChunkLengths> length,
            CountWhereBucketedContext ctx) {
        final int chunkSize = destinations.size();

        final RowSet rows;
        if (chunkedWhereFilters.length > 0) {
            ctx.fullKeyChunk.setSize(chunkSize);
            if (isAnd) {
                chunkFilterAnd(chunkedWhereFilters, values, ctx.fullKeyChunk, ctx.workingValueChunk, ctx.outputKeyChunk);
            } else {
                chunkFilterOr(chunkedWhereFilters, values, ctx.fullKeyChunk, ctx.workingKeyChunk, ctx.workingValueChunk, ctx.outputKeyChunk);
            }
            if (whereFilters.length == 0) {
                // If there are zero non-chunk filters, update the output count chunk from the remaining row-keys
                if (ctx.outputKeyChunk.size() == 0) {
                    ctx.countChunk.fillWithValue(0, startPositions.size(), 0);
                    return;
                }
                int keyIndex = 0;
                int currentKey = (int)ctx.outputKeyChunk.get(keyIndex);
                for (int ii = 0; ii < startPositions.size(); ii++) {
                    int count = 0;
                    final int startIndex = startPositions.get(ii);
                    final int lastIndex = startIndex + length.get(ii);
                    while (currentKey >= startIndex && currentKey < lastIndex) {
                        count++;
                        if (++keyIndex >= ctx.outputKeyChunk.size()) {
                            currentKey = Integer.MAX_VALUE;
                            break;
                        }
                        currentKey = (int)ctx.outputKeyChunk.get(keyIndex);
                    }
                    ctx.countChunk.set(ii, count);
                }
                return;
            } else {
                // Turn our chunked results into a RowSet for the next round of filtering
                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                builder.appendOrderedRowKeysChunk(ctx.outputKeyChunk);
                rows = builder.build();
            }
        } else {
            // We'll use the full row set for the input to the filters
            rows = RowSetFactory.flat(chunkSize);
        }

        if (chunkColumnSource != null) {
            // Clear the chunk sources and add the new sliced chunk
            chunkColumnSource.clear();
            // TODO: Can this cast ever fail? Investigate.
            chunkColumnSource.addChunk((WritableChunk<? extends Values>) values);
        }

        RowSet result = rows;
        for (WhereFilter whereFilter : whereFilters) {
            try (final RowSet ignored2 = result) {
                result = whereFilter.filter(result, RowSetFactory.flat(chunkSize), chunkSourceTable, false);
            }
        }
        try (final RowSet ignored = result;
             final RowSequence.Iterator it = result.getRowSequenceIterator()) {
            for (int ii = 0; ii < startPositions.size(); ii++) {
                final int startIndex = startPositions.get(ii);
                final int lastIndex = startIndex + length.get(ii);

                // Count how many rows passed the filter for this destination
                final int count = (int) it.advanceAndGetPositionDistance(lastIndex);
                ctx.countChunk.set(ii, count);
            }
        }
    }

    private int doCountSingle(
            Chunk<? extends Values> values,
            int chunkSize,
            CountWhereSingletonContext ctx) {

        final RowSet rows;
        if (chunkedWhereFilters.length > 0) {
            ctx.fullKeyChunk.setSize(chunkSize);
            if (isAnd) {
                chunkFilterAnd(chunkedWhereFilters, values, ctx.fullKeyChunk, ctx.workingValueChunk, ctx.outputKeyChunk);
            } else {
                chunkFilterOr(chunkedWhereFilters, values, ctx.fullKeyChunk, ctx.workingKeyChunk, ctx.workingValueChunk, ctx.outputKeyChunk);
            }
            if (whereFilters.length == 0) {
                // If there are zero non-chunk filters, we can just return the size of the chunked results
                return ctx.outputKeyChunk.size();
            } else {
                // Turn our chunked results into a RowSet for the next round of filtering
                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                builder.appendOrderedRowKeysChunk(ctx.outputKeyChunk);
                rows = builder.build();
            }
        } else {
            // We'll use the full row set for the input to the filters
            rows = RowSetFactory.flat(chunkSize);
        }

        if (chunkColumnSource != null) {
            // Clear the chunk sources and add the new sliced chunk
            chunkColumnSource.clear();
            // TODO: Can this cast ever fail? Investigate.
            chunkColumnSource.addChunk((WritableChunk<? extends Values>) values);
        }

        RowSet result = rows;
        for (WhereFilter whereFilter : whereFilters) {
            try (final RowSet ignored2 = result) {
                result = whereFilter.filter(result, RowSetFactory.flat(chunkSize), chunkSourceTable, false);
            }
        }
        try (final RowSet ignored2 = result) {
            return result.intSize();
        }
    }

    @Override
    public void addChunk(
            BucketedContext context,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys,
            IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions,
            IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {

        final CountWhereBucketedContext ctx = (CountWhereBucketedContext) context;

        // Do the counting work and store the results in the count chunk
        doCountMultiple(values, destinations, startPositions, length, ctx);

        // Update the result column source with the counts
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int count = ctx.countChunk.get(ii);
            final long destination = destinations.get(startPosition);
            resultColumnSource.set(destination, plusLong(resultColumnSource.getUnsafe(destination), count));
            stateModified.set(ii, count > 0);
        }
    }

    @Override
    public boolean addChunk(
            SingletonContext context,
            int chunkSize,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {

        final CountWhereSingletonContext ctx = (CountWhereSingletonContext) context;

        final int count = doCountSingle(values, chunkSize, ctx);
        resultColumnSource.set(destination, plusLong(resultColumnSource.getUnsafe(destination), count));
        return count > 0;
    }

    @Override
    public void removeChunk(
            BucketedContext context,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys,
            IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions,
            IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(values, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean removeChunk(
            SingletonContext context,
            int chunkSize,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys,
            long destination) {
        return removeChunk(values, destination, 0, values == null ? 0 : values.size());
    }

    private boolean removeChunk(
            Chunk<? extends Values> values,
            long destination,
            int chunkStart,
            int chunkSize) {
        final int count = doCount(chunkStart, chunkSize, values);
        if (count > 0) {
            final long updatedCount = plusLong(resultColumnSource.getUnsafe(destination), -count);
            Assert.geqZero(updatedCount, "updatedCount");
            resultColumnSource.set(destination, updatedCount);
            return true;
        }
        return false;
    }

    @Override
    public void modifyChunk(
            BucketedContext context,
            Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues,
            LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions,
            IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii,
                    modifyChunk(previousValues, newValues, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean modifyChunk(
            SingletonContext context,
            int chunkSize,
            Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues,
            LongChunk<? extends RowKeys> postShiftRowKeys,
            long destination) {
        return modifyChunk(previousValues, newValues, destination, 0, newValues == null ? 0 : newValues.size());
    }

    private boolean modifyChunk(
            Chunk<? extends Values> oldValues,
            Chunk<? extends Values> newValues,
            long destination,
            int chunkStart,
            int chunkSize) {
        final int oldCount = doCount(chunkStart, chunkSize, oldValues);
        final int newCount = doCount(chunkStart, chunkSize, newValues);
        final int count = newCount - oldCount;
        if (count != 0) {
            final long updatedCount = plusLong(resultColumnSource.getUnsafe(destination), count);
            Assert.geqZero(updatedCount, "updatedCount");
            resultColumnSource.set(destination, updatedCount);
            return true;
        }
        return false;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumnSource.ensureCapacity(tableSize, false);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.singletonMap(resultName, resultColumnSource);
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumnSource.startTrackingPrevValues();
    }

    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new CountWhereBucketedContext(size, chunkColumnSource == null ? null : chunkColumnSource.getChunkType());
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new CountWhereSingletonContext(size, chunkColumnSource == null ? null : chunkColumnSource.getChunkType());
    }
}
