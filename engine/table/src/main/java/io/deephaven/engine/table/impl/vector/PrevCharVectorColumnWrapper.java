/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.vector;

import io.deephaven.base.ClampUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.vector.CharVector;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

public class PrevCharVectorColumnWrapper extends CharVector.Indirect {

    private final ColumnSource<Character> columnSource;
    private final RowSet rowSet;
    private final long startPadding;
    private final long endPadding;

    public PrevCharVectorColumnWrapper(@NotNull final ColumnSource<Character> columnSource,
                                       @NotNull final RowSet rowSet) {
        this(columnSource, rowSet, 0, 0);
    }

    private PrevCharVectorColumnWrapper(@NotNull final ColumnSource<Character> columnSource,
                                        @NotNull final RowSet rowSet, final long startPadding, final long endPadding) {
        Assert.neqNull(rowSet, "rowSet");
        this.columnSource = columnSource;
        this.rowSet = rowSet;
        this.startPadding = startPadding;
        this.endPadding = endPadding;
    }

    @Override
    public char get(long index) {
        index -= startPadding;

        if (index < 0 || index > rowSet.size() - 1) {
            return NULL_CHAR;
        }

        return columnSource.getPrevChar(rowSet.get(index));
    }

    @Override
    public CharVector subVector(long fromIndexInclusive, long toIndexExclusive) {
        fromIndexInclusive -= startPadding;
        toIndexExclusive -= startPadding;

        final long realFrom = ClampUtil.clampLong(0, rowSet.size(), fromIndexInclusive);
        final long realTo = ClampUtil.clampLong(0, rowSet.size(), toIndexExclusive);

        long newStartPadding = toIndexExclusive < 0 ? toIndexExclusive - fromIndexInclusive : Math.max(0, -fromIndexInclusive);
        long newEndPadding = fromIndexInclusive >= rowSet.size() ? toIndexExclusive - fromIndexInclusive : Math.max(0, toIndexExclusive - rowSet.size());

        return new PrevCharVectorColumnWrapper(columnSource, rowSet.subSetByPositionRange(realFrom, realTo),
                newStartPadding, newEndPadding);
    }

    @Override
    public CharVector subVectorByPositions(long[] positions) {
        RowSetBuilderRandom builder = RowSetFactory.builderRandom();

        for (long position : positions) {
            final long realPos = position - startPadding;

            if (realPos < rowSet.size()) {
                builder.addKey(rowSet.get(realPos));
            }
        }

        return new PrevCharVectorColumnWrapper(columnSource, builder.build(), 0, 0);
    }

    @Override
    public char[] toArray() {
        return toArray(false, Integer.MAX_VALUE);
    }

    public char[] toArray(boolean shouldBeNullIfOutofBounds, int maxSize) {
        if (shouldBeNullIfOutofBounds && (startPadding > 0 || endPadding > 0)) {
            return null;
        }

        long sz = Math.min(size(), maxSize);

        char[] result = new char[LongSizedDataStructure.intSize("toArray", sz)];
        for (int i = 0; i < sz; i++) {
            result[i] = get(i);
        }

        return result;
    }

    @Override
    public long size() {
        return startPadding + rowSet.size() + endPadding;
    }

}
