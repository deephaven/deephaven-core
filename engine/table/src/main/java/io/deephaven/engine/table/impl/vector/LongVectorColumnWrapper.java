/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorColumnWrapper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.vector;

import io.deephaven.base.ClampUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.vector.LongVector;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class LongVectorColumnWrapper extends LongVector.Indirect {

    private static final long serialVersionUID = -2715269662143763674L;

    private final ColumnSource<Long> columnSource;
    private final RowSet rowSet;
    private final long startPadding;
    private final long endPadding;

    public LongVectorColumnWrapper(@NotNull final ColumnSource<Long> columnSource, @NotNull final RowSet rowSet) {
        this(columnSource, rowSet, 0, 0);
    }

    public LongVectorColumnWrapper(@NotNull final ColumnSource<Long> columnSource, @NotNull final RowSet rowSet,
                                   final long startPadding, final long endPadding) {
        Assert.neqNull(rowSet, "rowSet");
        this.columnSource = columnSource;
        this.rowSet = rowSet;
        this.startPadding = startPadding;
        this.endPadding = endPadding;
    }

    @Override
    public long get(long i) {
        i-= startPadding;

        if (i<0 || i> rowSet.size()-1) {
            return NULL_LONG;
        }

        return columnSource.getLong(rowSet.get(i));
    }

    @Override
    public LongVector subVector(long fromIndex, long toIndex) {
        fromIndex-=startPadding;
        toIndex-=startPadding;

        final long realFrom = ClampUtil.clampLong(0, rowSet.size(), fromIndex);
        final long realTo = ClampUtil.clampLong(0, rowSet.size(), toIndex);

        long newStartPadding=toIndex<0 ? toIndex-fromIndex : Math.max(0, -fromIndex);
        long newEndPadding= fromIndex>= rowSet.size() ? toIndex-fromIndex : Math.max(0, toIndex - rowSet.size());

        return new LongVectorColumnWrapper(columnSource, rowSet.subSetByPositionRange(realFrom, realTo), newStartPadding, newEndPadding);
    }

    @Override
    public LongVector subVectorByPositions(long [] positions) {
        RowSetBuilderRandom builder = RowSetFactory.builderRandom();

        for (long position : positions) {
            final long realPos = position - startPadding;

            if (realPos < rowSet.size()) {
                builder.addKey(rowSet.get(realPos));
            }
        }

        return new LongVectorColumnWrapper(columnSource, builder.build(), 0, 0);
    }

    @Override
    public long[] toArray() {
        return toArray(false,Integer.MAX_VALUE);
    }

    public long[] toArray(boolean shouldBeNullIfOutofBounds,int maxSize) {
        if (shouldBeNullIfOutofBounds && (startPadding>0 || endPadding>0)){
            return null;
        }

        int sz = LongSizedDataStructure.intSize("toArray", Math.min(size(),maxSize));

        long[] result = new long[sz];
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
