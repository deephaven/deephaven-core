/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit PrevCharVectorColumnWrapper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.vector;

import io.deephaven.base.ClampUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.vector.IntVector;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_INT;

public class PrevIntVectorColumnWrapper extends IntVector.Indirect {

    private static final long serialVersionUID = -2715269662143763674L;

    private final ColumnSource<Integer> columnSource;
    private final RowSet rowSet;
    private final long startPadding;
    private final long endPadding;

    public PrevIntVectorColumnWrapper(@NotNull final ColumnSource<Integer> columnSource,
                                       @NotNull final RowSet rowSet) {
        this(columnSource, rowSet, 0, 0);
    }

    private PrevIntVectorColumnWrapper(@NotNull final ColumnSource<Integer> columnSource,
                                        @NotNull final RowSet rowSet, final long startPadding, final long endPadding) {
        Assert.neqNull(rowSet, "rowSet");
        this.columnSource = columnSource;
        this.rowSet = rowSet;
        this.startPadding = startPadding;
        this.endPadding = endPadding;
    }

    @Override
    public int get(long i) {
        i -= startPadding;

        if (i < 0 || i > rowSet.size() - 1) {
            return NULL_INT;
        }

        return columnSource.getPrevInt(rowSet.get(i));
    }

    @Override
    public IntVector subVector(long fromIndex, long toIndex) {
        fromIndex -= startPadding;
        toIndex -= startPadding;

        final long realFrom = ClampUtil.clampLong(0, rowSet.size(), fromIndex);
        final long realTo = ClampUtil.clampLong(0, rowSet.size(), toIndex);

        long newStartPadding = toIndex < 0 ? toIndex - fromIndex : Math.max(0, -fromIndex);
        long newEndPadding = fromIndex >= rowSet.size() ? toIndex - fromIndex : Math.max(0, toIndex - rowSet.size());

        return new PrevIntVectorColumnWrapper(columnSource, rowSet.subSetByPositionRange(realFrom, realTo),
                newStartPadding, newEndPadding);
    }

    @Override
    public IntVector subVectorByPositions(long[] positions) {
        RowSetBuilderRandom builder = RowSetFactory.builderRandom();

        for (long position : positions) {
            final long realPos = position - startPadding;

            if (realPos < rowSet.size()) {
                builder.addKey(rowSet.get(realPos));
            }
        }

        return new PrevIntVectorColumnWrapper(columnSource, builder.build(), 0, 0);
    }

    @Override
    public int[] toArray() {
        return toArray(false, Integer.MAX_VALUE);
    }

    public int[] toArray(boolean shouldBeNullIfOutofBounds, int maxSize) {
        if (shouldBeNullIfOutofBounds && (startPadding > 0 || endPadding > 0)) {
            return null;
        }

        long sz = Math.min(size(), maxSize);

        int[] result = new int[LongSizedDataStructure.intSize("toArray", sz)];
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
