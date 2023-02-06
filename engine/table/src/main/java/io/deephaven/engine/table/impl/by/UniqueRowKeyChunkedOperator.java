/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class UniqueRowKeyChunkedOperator
        extends BasicStateChangeRecorder
        implements IterativeChunkedAggregationOperator {

    private final String resultColumnName;
    private final LongArraySource rowKeys;

    public UniqueRowKeyChunkedOperator(@NotNull final String resultColumnName) {
        this.resultColumnName = resultColumnName;
        rowKeys = new LongArraySource();
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            stateModified.set(ii, addChunk(inputRowKeysAsOrdered, startPosition, runLength, destination));
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            stateModified.set(ii, removeChunk(inputRowKeysAsOrdered, startPosition, runLength, destination));
        }
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // We have no inputs, so should never get here.
        throw new IllegalStateException();
    }

    @Override
    public void shiftChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> preShiftRowKeys,
            LongChunk<? extends RowKeys> postShiftRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> preShiftRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) preShiftRowKeys;
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> postShiftRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) postShiftRowKeys;

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            stateModified.set(ii, doShift(preShiftRowKeysAsOrdered, postShiftRowKeysAsOrdered,
                    startPosition, runLength, destination));
        }
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        // noinspection unchecked
        return addChunk((LongChunk<OrderedRowKeys>) inputRowKeys, 0, inputRowKeys.size(), destination);
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        // noinspection unchecked
        return removeChunk((LongChunk<OrderedRowKeys>) inputRowKeys, 0, inputRowKeys.size(), destination);
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        // We have no inputs, so should never get here.
        throw new IllegalStateException();
    }

    @Override
    public boolean shiftChunk(SingletonContext singletonContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> preShiftRowKeys,
            LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        // noinspection unchecked
        return doShift((LongChunk<OrderedRowKeys>) preShiftRowKeys, (LongChunk<OrderedRowKeys>) postShiftRowKeys,
                0, preShiftRowKeys.size(), destination);
    }

    private boolean addChunk(
            @NotNull final LongChunk<OrderedRowKeys> rowKeys, final int start, final int length,
            final long destination) {
        if (length != 1) {
            throw new IllegalStateException(String.format(
                    "Adding invalid number (%d) of \"unique\" row keys for destination %d", length, destination));
        }
        return updateRowKey(destination, NULL_LONG, rowKeys.get(start));
    }

    @Override
    public boolean addRowSet(@Nullable final SingletonContext context, @NotNull final RowSet addRowSet,
            final long destination) {
        if (addRowSet.isEmpty()) {
            return false;
        }
        if (addRowSet.size() > 1) {
            throw new UnsupportedOperationException(String.format(
                    "Adding multiple (%s) \"unique\" row keys for destination %d", addRowSet, destination));
        }
        return updateRowKey(destination, NULL_LONG, addRowSet.firstRowKey());
    }

    private boolean removeChunk(@NotNull final LongChunk<OrderedRowKeys> rowKeys,
            final int start, final int length, final long destination) {
        if (length != 1) {
            throw new IllegalStateException(String.format(
                    "Removing invalid number (%d) of \"unique\" row keys for destination %d", length, destination));
        }
        return updateRowKey(destination, rowKeys.get(start), NULL_LONG);
    }

    private boolean doShift(
            @NotNull final LongChunk<OrderedRowKeys> preShiftRowKeys,
            @NotNull final LongChunk<OrderedRowKeys> postShiftRowKeys,
            final int start, final int length, final long destination) {
        if (length != 1) {
            throw new IllegalStateException(String.format(
                    "Shifting invalid number (%d) of \"unique\" row keys for destination %d", length, destination));
        }
        return updateRowKey(destination, preShiftRowKeys.get(start), postShiftRowKeys.get(start));
    }

    private boolean updateRowKey(final long destination, final long expectedOldRowKey, final long newRowKey) {
        final long oldRowKey = rowKeys.getAndSetUnsafe(destination, newRowKey);
        if (oldRowKey != expectedOldRowKey) {
            final String expected = rowKeyToString(expectedOldRowKey);
            final String found = rowKeyToString(oldRowKey);
            final String inserted = rowKeyToString(newRowKey);
            throw new UnsupportedOperationException(String.format(
                    "Unique row key invariant violated at destination %d: expected to replace %s with %s, but found %s instead",
                    destination, expected, inserted, found));
        }
        if (oldRowKey == NULL_LONG) {
            Assert.neq(newRowKey, "newRowKey", NULL_LONG, "NULL_LONG");
            onReincarnated(destination);
        }
        if (newRowKey == NULL_LONG) {
            onEmptied(destination);
        }
        return oldRowKey != newRowKey;
    }

    @NotNull
    private static String rowKeyToString(long expectedOldRowKey) {
        return expectedOldRowKey == NULL_LONG ? "null" : Long.toString(expectedOldRowKey);
    }

    @Override
    public boolean unchunkedRowSet() {
        return true;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        rowKeys.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Map.of(resultColumnName, rowKeys);
    }

    @Override
    public void startTrackingPrevValues() {
        rowKeys.startTrackingPrevValues();
    }

    @Override
    public boolean requiresRowKeys() {
        return true;
    }
}
