/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit PrevCharVectorColumnWrapper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.dbarrays;

import io.deephaven.base.ClampUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.vector.DoubleVector;
import io.deephaven.engine.util.LongSizedDataStructure;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.utils.RowSet;
import io.deephaven.engine.v2.utils.RowSetBuilderRandom;
import io.deephaven.engine.v2.utils.RowSetFactory;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class PrevDoubleVectorColumnWrapper extends DoubleVector.Indirect {

    private static final long serialVersionUID = -2715269662143763674L;

    private final ColumnSource<Double> columnSource;
    private final RowSet rowSet;
    private final long startPadding;
    private final long endPadding;

    public PrevDoubleVectorColumnWrapper(@NotNull final ColumnSource<Double> columnSource,
                                       @NotNull final RowSet rowSet) {
        this(columnSource, rowSet, 0, 0);
    }

    private PrevDoubleVectorColumnWrapper(@NotNull final ColumnSource<Double> columnSource,
                                        @NotNull final RowSet rowSet, final long startPadding, final long endPadding) {
        Assert.neqNull(rowSet, "rowSet");
        this.columnSource = columnSource;
        this.rowSet = rowSet;
        this.startPadding = startPadding;
        this.endPadding = endPadding;
    }

    @Override
    public double get(long i) {
        i -= startPadding;

        if (i < 0 || i > rowSet.size() - 1) {
            return NULL_DOUBLE;
        }

        return columnSource.getPrevDouble(rowSet.get(i));
    }

    @Override
    public DoubleVector subVector(long fromIndex, long toIndex) {
        fromIndex -= startPadding;
        toIndex -= startPadding;

        final long realFrom = ClampUtil.clampLong(0, rowSet.size(), fromIndex);
        final long realTo = ClampUtil.clampLong(0, rowSet.size(), toIndex);

        long newStartPadding = toIndex < 0 ? toIndex - fromIndex : Math.max(0, -fromIndex);
        long newEndPadding = fromIndex >= rowSet.size() ? toIndex - fromIndex : Math.max(0, toIndex - rowSet.size());

        return new PrevDoubleVectorColumnWrapper(columnSource, rowSet.subSetByPositionRange(realFrom, realTo),
                newStartPadding, newEndPadding);
    }

    @Override
    public DoubleVector subVectorByPositions(long[] positions) {
        RowSetBuilderRandom builder = RowSetFactory.builderRandom();

        for (long position : positions) {
            final long realPos = position - startPadding;

            if (realPos < rowSet.size()) {
                builder.addKey(rowSet.get(realPos));
            }
        }

        return new PrevDoubleVectorColumnWrapper(columnSource, builder.build(), 0, 0);
    }

    @Override
    public double[] toArray() {
        return toArray(false, Integer.MAX_VALUE);
    }

    public double[] toArray(boolean shouldBeNullIfOutofBounds, int maxSize) {
        if (shouldBeNullIfOutofBounds && (startPadding > 0 || endPadding > 0)) {
            return null;
        }

        long sz = Math.min(size(), maxSize);

        double[] result = new double[LongSizedDataStructure.intSize("toArray", sz)];
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
