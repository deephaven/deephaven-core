/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.vector;

import io.deephaven.base.ClampUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.table.iterators.ObjectColumnIterator;
import io.deephaven.vector.ObjectSubVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Arrays;

import static io.deephaven.engine.primitive.iterator.CloseableIterator.maybeConcat;
import static io.deephaven.engine.primitive.iterator.CloseableIterator.repeat;
import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.engine.table.iterators.ColumnIterator.DEFAULT_CHUNK_SIZE;

public class ObjectVectorColumnWrapper<T> extends ObjectVector.Indirect<T> {

    private final ColumnSource<T> columnSource;
    private final RowSet rowSet;
    private final long startPadding;
    private final long endPadding;

    public ObjectVectorColumnWrapper(
            @NotNull final ColumnSource<T> columnSource,
            @NotNull final RowSet rowSet) {
        this(columnSource, rowSet, 0, 0);
    }

    public ObjectVectorColumnWrapper(
            @NotNull final ColumnSource<T> columnSource,
            @NotNull final RowSet rowSet,
            final long startPadding,
            final long endPadding) {
        Assert.neqNull(rowSet, "rowSet");
        this.columnSource = columnSource;
        this.rowSet = rowSet;
        this.startPadding = startPadding;
        this.endPadding = endPadding;
    }

    @Override
    public T get(long index) {
        index -= startPadding;

        if (index < 0 || index >= rowSet.size()) {
            return null;
        }

        return columnSource.get(rowSet.get(index));
    }

    @Override
    public ObjectVector<T> subVector(long fromIndexInclusive, long toIndexExclusive) {
        fromIndexInclusive -= startPadding;
        toIndexExclusive -= startPadding;

        final long realFrom = ClampUtil.clampLong(0, rowSet.size(), fromIndexInclusive);
        final long realTo = ClampUtil.clampLong(0, rowSet.size(), toIndexExclusive);

        final long newStartPadding = toIndexExclusive < 0
                ? toIndexExclusive - fromIndexInclusive
                : Math.max(0, -fromIndexInclusive);
        final long newEndPadding = fromIndexInclusive >= rowSet.size()
                ? toIndexExclusive - fromIndexInclusive
                : Math.max(0, toIndexExclusive - rowSet.size());

        return new ObjectVectorColumnWrapper<>(columnSource, rowSet.subSetByPositionRange(realFrom, realTo),
                newStartPadding, newEndPadding);
    }

    public ObjectVector<T> subVectorByPositions(@NotNull final long[] positions) {
        return new ObjectSubVector<>(this, positions);
    }

    public T[] toArray(final boolean shouldBeNullIfOutOfBounds, final long maxSize) {
        if (shouldBeNullIfOutOfBounds && (startPadding > 0 || endPadding > 0)) {
            return null;
        }

        final int size = (int) Math.min(size(), maxSize);
        // noinspection unchecked
        final T[] result = (T[]) Array.newInstance(getComponentType(), size);
        int nextFillIndex;

        final int startPaddingFillAmount = (int) Math.min(startPadding, size);
        if (startPaddingFillAmount > 0) {
            Arrays.fill(result, 0, startPaddingFillAmount, null);
            nextFillIndex = startPaddingFillAmount;
        } else {
            nextFillIndex = 0;
        }

        final int rowSetFillAmount = (int) Math.min(rowSet.size(), size - nextFillIndex);
        if (rowSetFillAmount > 0) {
            try (final ObjectColumnIterator<T> iterator = new ObjectColumnIterator<>(columnSource, rowSet,
                    DEFAULT_CHUNK_SIZE, rowSet.firstRowKey(), rowSetFillAmount)) {
                for (int ri = 0; ri < rowSetFillAmount; ++ri) {
                    result[nextFillIndex++] = iterator.next();
                }
            }
        }

        final int endPaddingFillAmount = (int) Math.min(endPadding, size - nextFillIndex);
        if (endPaddingFillAmount > 0) {
            Arrays.fill(result, nextFillIndex, nextFillIndex + endPaddingFillAmount, null);
        }

        return result;
    }

    @Override
    public CloseableIterator<T> iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        final long rowSetSize = rowSet.size();
        if (startPadding == 0 && endPadding == 0 && fromIndexInclusive == 0 && toIndexExclusive == rowSetSize) {
            return new ObjectColumnIterator<>(columnSource, rowSet, DEFAULT_CHUNK_SIZE, rowSet.firstRowKey(),
                    rowSetSize);
        }

        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");

        final long totalWanted = toIndexExclusive - fromIndexInclusive;
        final long includedInitialNulls = fromIndexInclusive < startPadding
                ? Math.min(startPadding - fromIndexInclusive, totalWanted)
                : 0;
        long remaining = totalWanted - includedInitialNulls;

        final long firstIncludedRowKey;
        final long includedRows;
        if (remaining > 0 && rowSetSize > 0 && fromIndexInclusive < startPadding + rowSetSize) {
            if (fromIndexInclusive <= startPadding) {
                firstIncludedRowKey = rowSet.firstRowKey();
                includedRows = Math.min(rowSetSize, remaining);
            } else {
                final long firstIncludedRowPosition = fromIndexInclusive - startPadding;
                firstIncludedRowKey = rowSet.get(firstIncludedRowPosition);
                includedRows = Math.min(rowSetSize - firstIncludedRowPosition, remaining);
            }
            remaining -= includedRows;
        } else {
            firstIncludedRowKey = NULL_ROW_KEY;
            includedRows = 0;
        }

        final CloseableIterator<T> initialNullsIterator = includedInitialNulls > 0
                ? repeat(null, includedInitialNulls)
                : null;
        final CloseableIterator<T> rowsIterator = includedRows > 0
                ? new ObjectColumnIterator<>(columnSource, rowSet, DEFAULT_CHUNK_SIZE, firstIncludedRowKey,
                        includedRows)
                : null;
        final CloseableIterator<T> finalNullsIterator = remaining > 0
                ? repeat(null, remaining)
                : null;
        return maybeConcat(initialNullsIterator, rowsIterator, finalNullsIterator);
    }

    @Override
    public long size() {
        return startPadding + rowSet.size() + endPadding;
    }

    @Override
    public Class<T> getComponentType() {
        return columnSource.getType();
    }
}
