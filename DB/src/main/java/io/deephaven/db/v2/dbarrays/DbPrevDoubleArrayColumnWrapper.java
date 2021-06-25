/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbPrevCharArrayColumnWrapper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.dbarrays;

import io.deephaven.base.ClampUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.tables.dbarrays.*;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexBuilder;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DbPrevDoubleArrayColumnWrapper extends DbDoubleArray.Indirect {

    private static final long serialVersionUID = -2715269662143763674L;

    private final ColumnSource<Double> columnSource;
    private final Index index;
    private final long startPadding;
    private final long endPadding;

    public DbPrevDoubleArrayColumnWrapper(@NotNull final ColumnSource<Double> columnSource, @NotNull final Index index) {
        this(columnSource, index, 0, 0);
    }

    public DbPrevDoubleArrayColumnWrapper(@NotNull final ColumnSource<Double> columnSource, @NotNull final Index index,
                                        final long startPadding, final long endPadding) {
        this(columnSource, index, startPadding, endPadding, false);
    }

    private DbPrevDoubleArrayColumnWrapper(@NotNull final ColumnSource<Double> columnSource, @NotNull final Index index,
                                         final long startPadding, final long endPadding, final boolean alreadyPrevIndex) {
        Assert.neqNull(index, "index");
        this.columnSource = columnSource;
        this.index = alreadyPrevIndex ? index : index.getPrevIndex();
        this.startPadding = startPadding;
        this.endPadding = endPadding;
    }

    @Override
    public double get(long i) {
        i-= startPadding;

        if (i<0 || i> index.size()-1) {
            return NULL_DOUBLE;
        }

        return columnSource.getPrevDouble(index.get(i));
    }

    @Override
    public double getPrev(long offset) {
        return get(offset);
    }

    @Override
    public DbDoubleArray subArray(long fromIndex, long toIndex) {
        fromIndex-=startPadding;
        toIndex-=startPadding;

        final long realFrom = ClampUtil.clampLong(0, index.size(), fromIndex);
        final long realTo = ClampUtil.clampLong(0, index.size(), toIndex);

        long newStartPadding=toIndex<0 ? toIndex-fromIndex : Math.max(0, -fromIndex);
        long newEndPadding= fromIndex>= index.size() ? toIndex-fromIndex : Math.max(0, toIndex - index.size());

        return new DbPrevDoubleArrayColumnWrapper(columnSource, index.subindexByPos(realFrom, realTo), newStartPadding, newEndPadding, true);
    }

    @Override
    public DbDoubleArray subArrayByPositions(long[] positions) {
        IndexBuilder builder = Index.FACTORY.getRandomBuilder();

        for (long position : positions) {
            final long realPos = position - startPadding;

            if (realPos < index.size()) {
                builder.addKey(index.get(realPos));
            }
        }

        return new DbPrevDoubleArrayColumnWrapper(columnSource, builder.getIndex(), 0, 0);
    }

    @Override
    public double[] toArray() {
        return toArray(false,Integer.MAX_VALUE);
    }

    public double[] toArray(boolean shouldBeNullIfOutofBounds,int maxSize) {
        if (shouldBeNullIfOutofBounds && (startPadding>0 || endPadding>0)){
            return null;
        }

        long sz=Math.min(size(),maxSize);

        double[] result = new double[LongSizedDataStructure.intSize("toArray", sz)];
        for (int i = 0; i < sz; i++) {
            result[i] = get(i);
        }
        
        return result;
    }

    @Override
    public long size() {
        return startPadding + index.size() + endPadding;
    }

}
