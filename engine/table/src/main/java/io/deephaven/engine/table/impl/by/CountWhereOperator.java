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
import java.util.Map;

import static io.deephaven.engine.util.NullSafeAddition.plusLong;
import static io.deephaven.engine.util.TableTools.emptyTable;

public class CountWhereOperator implements IterativeChunkedAggregationOperator {
    private final String resultName;
    final LongArraySource resultColumnSource;
    final WhereFilter[] whereFilters;
    final ExposesChunkFilter[] chunkWhereFilters;

    /**
     * A table constructed from chunk sources, will be populated with incoming chunk data and used to evaluate filters.
     */
    final Table chunkSourceTable;
    final ChunkColumnSource<?> chunkColumnSource;

    static class CountWhereBucketedContext implements BucketedContext {
        WritableLongChunk<OrderedRowKeys> startingKeyChunk;
        WritableLongChunk<OrderedRowKeys> workingKeyChunk;
        WritableIntChunk<Values> countChunk;

        public CountWhereBucketedContext(int size) {
            workingKeyChunk = WritableLongChunk.makeWritableChunk(size);
            startingKeyChunk = WritableLongChunk.makeWritableChunk(size);
            for (int ii = 0; ii < size; ++ii) {
                startingKeyChunk.set(ii, ii);
            }
            countChunk = WritableIntChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            workingKeyChunk.close();
            startingKeyChunk.close();
            countChunk.close();
        }
    }

    static class CountWhereSingletonContext implements SingletonContext {
        WritableLongChunk<OrderedRowKeys> startingKeyChunk;
        WritableLongChunk<OrderedRowKeys> workingKeyChunk;

        public CountWhereSingletonContext(int size) {
            workingKeyChunk = WritableLongChunk.makeWritableChunk(size);
            startingKeyChunk = WritableLongChunk.makeWritableChunk(size);
            for (int ii = 0; ii < size; ++ii) {
                startingKeyChunk.set(ii, ii);
            }
        }

        @Override
        public void close() {
            workingKeyChunk.close();
            startingKeyChunk.close();
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

        this.whereFilters = Arrays.stream(whereFilters)
                .filter(filter -> !(filter instanceof ExposesChunkFilter))
                .toArray(WhereFilter[]::new);

        this.chunkWhereFilters = Arrays.stream(whereFilters)
                .filter(ExposesChunkFilter.class::isInstance)
                .map(ExposesChunkFilter.class::cast)
                .toArray(ExposesChunkFilter[]::new);

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
        if (chunkColumnSource != null) {
            // Clear the chunk sources and add the new sliced chunk
            chunkColumnSource.clear();
            // TODO: Can this cast ever fail? Investigate.
            chunkColumnSource.addChunk((WritableChunk<? extends Values>) values);
        }
        final int chunkSize = destinations.size();


        // NOTE: we don't need to modify the table RowSet since we are supplying the filters directly
        try (final RowSet fullRowSet = RowSetFactory.flat(chunkSize)) {
            // Apply the filters successively to the chunk source table
            RowSet result = RowSetFactory.flat(chunkSize);
            for (WhereFilter whereFilter : whereFilters) {
                result = whereFilter.filter(result, fullRowSet, chunkSourceTable, false);
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
    }

    private int doCountSingle(
            Chunk<? extends Values> values,
            int chunkSize,
            CountWhereSingletonContext ctx) {
        if (chunkColumnSource != null) {
            // Clear the chunk sources and add the new sliced chunk
            chunkColumnSource.clear();
            // TODO: Can this cast ever fail? Investigate.
            chunkColumnSource.addChunk((WritableChunk<? extends Values>) values);
        }

        // NOTE: we don't need to modify the table RowSet since we are supplying the filters directly
        try (final RowSet fullRowSet = RowSetFactory.flat(chunkSize)) {
            if (chunkWhereFilters.length > 0 && whereFilters.length > 0) {
                // These are cheap and fast, run them first to reduce the number of rows we need to process
                ctx.startingKeyChunk.setSize(chunkSize);
                chunkWhereFilters[0].chunkFilter().filter(values, ctx.startingKeyChunk, ctx.workingKeyChunk);
                for (int ii = 1; ii < chunkWhereFilters.length; ii++) {
                    chunkWhereFilters[ii].chunkFilter().filter(values, ctx.workingKeyChunk, ctx.workingKeyChunk);
                }
                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                builder.appendOrderedRowKeysChunk(ctx.workingKeyChunk);
                RowSet result = builder.build();
                for (WhereFilter whereFilter : whereFilters) {
                    result = whereFilter.filter(result, fullRowSet, chunkSourceTable, false);
                }
                return ctx.workingKeyChunk.size();
            } else if (chunkWhereFilters.length > 0) {
                ctx.startingKeyChunk.setSize(chunkSize);
                chunkWhereFilters[0].chunkFilter().filter(values, ctx.startingKeyChunk, ctx.workingKeyChunk);
                for (int ii = 1; ii < chunkWhereFilters.length; ii++) {
                    chunkWhereFilters[ii].chunkFilter().filter(values, ctx.workingKeyChunk, ctx.workingKeyChunk);
                }
                return ctx.workingKeyChunk.size();
            } else {
                RowSet result = RowSetFactory.flat(chunkSize);
                for (WhereFilter whereFilter : whereFilters) {
                    result = whereFilter.filter(result, fullRowSet, chunkSourceTable, false);
                }

                try (final RowSet ignored = result) {
                    return result.intSize();
                }
            }
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
        return new CountWhereBucketedContext(size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new CountWhereSingletonContext(size);
    }
}
