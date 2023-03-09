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
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.vector.IntVector;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_INT;

public class IntVectorColumnWrapper extends IntVector.Indirect {

    private final ColumnSource<Integer> columnSource;
    private final RowSet rowSet;
    private final long startPadding;
    private final long endPadding;

    public IntVectorColumnWrapper(@NotNull final ColumnSource<Integer> columnSource, @NotNull final RowSet rowSet) {
        this(columnSource, rowSet, 0, 0);
    }

    public IntVectorColumnWrapper(@NotNull final ColumnSource<Integer> columnSource, @NotNull final RowSet rowSet,
                                   final long startPadding, final long endPadding) {
        Assert.neqNull(rowSet, "rowSet");
        this.columnSource = columnSource;
        this.rowSet = rowSet;
        this.startPadding = startPadding;
        this.endPadding = endPadding;
    }

    @Override
    public int get(long index) {
        index -= startPadding;

        if (index <0 || index > rowSet.size()-1) {
            return NULL_INT;
        }

        return columnSource.getInt(rowSet.get(index));
    }

    @Override
    public IntVector subVector(long fromIndexInclusive, long toIndexExclusive) {
        fromIndexInclusive -=startPadding;
        toIndexExclusive -=startPadding;

        final long realFrom = ClampUtil.clampLong(0, rowSet.size(), fromIndexInclusive);
        final long realTo = ClampUtil.clampLong(0, rowSet.size(), toIndexExclusive);

        long newStartPadding= toIndexExclusive <0 ? toIndexExclusive - fromIndexInclusive : Math.max(0, -fromIndexInclusive);
        long newEndPadding= fromIndexInclusive >= rowSet.size() ? toIndexExclusive - fromIndexInclusive : Math.max(0, toIndexExclusive - rowSet.size());

        return new IntVectorColumnWrapper(columnSource, rowSet.subSetByPositionRange(realFrom, realTo), newStartPadding, newEndPadding);
    }

    @Override
    public IntVector subVectorByPositions(long [] positions) {
        RowSetBuilderRandom builder = RowSetFactory.builderRandom();

        for (long position : positions) {
            final long realPos = position - startPadding;

            if (realPos < rowSet.size()) {
                builder.addKey(rowSet.get(realPos));
            }
        }

        return new IntVectorColumnWrapper(columnSource, builder.build(), 0, 0);
    }

    @Override
    public int[] toArray() {
        return toArray(false,Integer.MAX_VALUE);
    }

    public int[] toArray(boolean shouldBeNullIfOutofBounds,int maxSize) {
        if (shouldBeNullIfOutofBounds && (startPadding>0 || endPadding>0)){
            return null;
        }

        int sz = LongSizedDataStructure.intSize("toArray", Math.min(size(),maxSize));

        int[] result = new int[sz];
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
