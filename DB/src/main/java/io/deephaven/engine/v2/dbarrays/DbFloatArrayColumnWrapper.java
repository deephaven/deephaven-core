/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbCharArrayColumnWrapper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.dbarrays;

import io.deephaven.base.ClampUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.util.LongSizedDataStructure;
import io.deephaven.engine.tables.dbarrays.*;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.utils.RowSet;
import io.deephaven.engine.v2.utils.RowSetFactoryImpl;
import io.deephaven.engine.v2.utils.RowSetBuilderRandom;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class DbFloatArrayColumnWrapper extends DbFloatArray.Indirect {

    private static final long serialVersionUID = -2715269662143763674L;

    private final ColumnSource<Float> columnSource;
    private final RowSet rowSet;
    private final long startPadding;
    private final long endPadding;

    public DbFloatArrayColumnWrapper(@NotNull final ColumnSource<Float> columnSource, @NotNull final RowSet rowSet){
        this(columnSource, rowSet, 0, 0);
    }

    public DbFloatArrayColumnWrapper(@NotNull final ColumnSource<Float> columnSource, @NotNull final RowSet rowSet,
                                    final long startPadding, final long endPadding){
        Assert.neqNull(rowSet, "rowSet");
        this.columnSource = columnSource;
        this.rowSet = rowSet;
        this.startPadding = startPadding;
        this.endPadding = endPadding;
    }

    @Override
    public float get(long i) {
        i-= startPadding;

        if (i<0 || i> rowSet.size()-1) {
            return NULL_FLOAT;
        }

        return columnSource.getFloat(rowSet.get(i));
    }

    @Override
    public float getPrev(long i) {
        i-= startPadding;

        if (i<0 || i> rowSet.size()-1) {
            return NULL_FLOAT;
        }

        return columnSource.getPrevFloat(rowSet.get(i));
    }

    @Override
    public DbFloatArray subArray(long fromIndex, long toIndex) {
        fromIndex-=startPadding;
        toIndex-=startPadding;

        final long realFrom = ClampUtil.clampLong(0, rowSet.size(), fromIndex);
        final long realTo = ClampUtil.clampLong(0, rowSet.size(), toIndex);

        long newStartPadding=toIndex<0 ? toIndex-fromIndex : Math.max(0, -fromIndex);
        long newEndPadding= fromIndex>= rowSet.size() ? toIndex-fromIndex : Math.max(0, toIndex - rowSet.size());

        return new DbFloatArrayColumnWrapper(columnSource, rowSet.subSetByPositionRange(realFrom, realTo), newStartPadding, newEndPadding);
    }

    @Override
    public DbFloatArray subArrayByPositions(long [] positions) {
        RowSetBuilderRandom builder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();

        for (long position : positions) {
            final long realPos = position - startPadding;

            if (realPos < rowSet.size()) {
                builder.addKey(rowSet.get(realPos));
            }
        }

        return new DbFloatArrayColumnWrapper(columnSource, builder.build(), 0, 0);
    }

    @Override
    public float[] toArray() {
        return toArray(false,Integer.MAX_VALUE);
    }

    public float[] toArray(boolean shouldBeNullIfOutofBounds,int maxSize) {
        if (shouldBeNullIfOutofBounds && (startPadding>0 || endPadding>0)){
            return null;
        }

        int sz = LongSizedDataStructure.intSize("toArray", Math.min(size(),maxSize));

        float[] result = new float[sz];
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
