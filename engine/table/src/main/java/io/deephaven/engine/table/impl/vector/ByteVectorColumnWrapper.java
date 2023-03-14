/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorColumnWrapper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.vector;

import io.deephaven.base.ClampUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfByte;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.iterators.ByteColumnIterator;
import io.deephaven.vector.ByteSubVector;
import io.deephaven.vector.ByteVector;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfByte.maybeConcat;
import static io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfByte.repeat;
import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.engine.table.iterators.ColumnIterator.DEFAULT_CHUNK_SIZE;
import static io.deephaven.util.QueryConstants.NULL_BYTE;

public class ByteVectorColumnWrapper extends ByteVector.Indirect {

    private final ColumnSource<Byte> columnSource;
    private final RowSet rowSet;
    private final long startPadding;
    private final long endPadding;

    public ByteVectorColumnWrapper(
            @NotNull final ColumnSource<Byte> columnSource,
            @NotNull final RowSet rowSet) {
        this(columnSource, rowSet, 0, 0);
    }

    public ByteVectorColumnWrapper(
            @NotNull final ColumnSource<Byte> columnSource,
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
    public byte get(long index) {
        index -= startPadding;

        if (index < 0 || index >= rowSet.size()) {
            return NULL_BYTE;
        }

        return columnSource.getByte(rowSet.get(index));
    }

    @Override
    public ByteVector subVector(long fromIndexInclusive, long toIndexExclusive) {
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

        return new ByteVectorColumnWrapper(columnSource, rowSet.subSetByPositionRange(realFrom, realTo),
                newStartPadding, newEndPadding);
    }

    @Override
    public ByteVector subVectorByPositions(final long[] positions) {
        return new ByteSubVector(this, positions);
    }

    public byte[] toArray(final boolean shouldBeNullIfOutOfBounds, final int maxSize) {
        if (shouldBeNullIfOutOfBounds && (startPadding > 0 || endPadding > 0)) {
            return null;
        }

        final int size = (int) Math.min(size(), maxSize);
        final byte[] result = new byte[size];
        int nextFillIndex;

        final int startPaddingFillAmount = (int) Math.min(startPadding, size);
        if (startPaddingFillAmount > 0) {
            Arrays.fill(result, 0, startPaddingFillAmount, NULL_BYTE);
            nextFillIndex = startPaddingFillAmount;
        } else {
            nextFillIndex = 0;
        }

        final int rowSetFillAmount = (int) Math.min(rowSet.size(), size - nextFillIndex);
        if (rowSetFillAmount > 0) {
            try (final ByteColumnIterator iterator = new ByteColumnIterator(columnSource, rowSet,
                    DEFAULT_CHUNK_SIZE, rowSet.firstRowKey(), rowSetFillAmount)) {
                for (int ri = 0; ri < rowSetFillAmount; ++ri) {
                    result[nextFillIndex++] = iterator.nextByte();
                }
            }
        }

        final int endPaddingFillAmount = (int) Math.min(endPadding, size - nextFillIndex);
        if (endPaddingFillAmount > 0) {
            Arrays.fill(result, nextFillIndex, nextFillIndex + endPaddingFillAmount, NULL_BYTE);
        }

        return result;
    }

    @Override
    public CloseablePrimitiveIteratorOfByte iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        final long rowSetSize = rowSet.size();
        if (startPadding == 0 && endPadding == 0 && fromIndexInclusive == 0 && toIndexExclusive == rowSetSize) {
            return new ByteColumnIterator(columnSource, rowSet, DEFAULT_CHUNK_SIZE,
                    rowSet.firstRowKey(), rowSetSize);
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

        final CloseablePrimitiveIteratorOfByte initialNullsIterator = includedInitialNulls > 0
                ? repeat(NULL_BYTE, includedInitialNulls)
                : null;
        final CloseablePrimitiveIteratorOfByte rowsIterator = includedRows > 0
                ? new ByteColumnIterator(columnSource, rowSet, DEFAULT_CHUNK_SIZE, firstIncludedRowKey,
                        includedRows)
                : null;
        final CloseablePrimitiveIteratorOfByte finalNullsIterator = remaining > 0
                ? repeat(NULL_BYTE, remaining)
                : null;
        return maybeConcat(initialNullsIterator, rowsIterator, finalNullsIterator);
    }

    @Override
    public long size() {
        return startPadding + rowSet.size() + endPadding;
    }
}
