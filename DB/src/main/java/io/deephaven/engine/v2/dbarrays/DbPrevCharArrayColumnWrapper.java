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
import io.deephaven.engine.v2.utils.TrackingRowSet;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

public class DbPrevCharArrayColumnWrapper extends DbCharArray.Indirect {

    private static final long serialVersionUID = -2715269662143763674L;

    private final ColumnSource<Character> columnSource;
    private final RowSet rowSet;
    private final long startPadding;
    private final long endPadding;

    public DbPrevCharArrayColumnWrapper(@NotNull final ColumnSource<Character> columnSource, @NotNull final TrackingRowSet rowSet) {
        this(columnSource, rowSet, 0, 0);
    }

    public DbPrevCharArrayColumnWrapper(@NotNull final ColumnSource<Character> columnSource, @NotNull final TrackingRowSet rowSet,
                                        final long startPadding, final long endPadding) {
        this(columnSource, rowSet, startPadding, endPadding, false);
    }

    private DbPrevCharArrayColumnWrapper(@NotNull final ColumnSource<Character> columnSource, @NotNull final TrackingRowSet rowSet,
                                         final long startPadding, final long endPadding, final boolean alreadyPrevIndex) {
        Assert.neqNull(rowSet, "rowSet");
        this.columnSource = columnSource;
        this.rowSet = alreadyPrevIndex ? rowSet : rowSet.getPrevRowSet();
        this.startPadding = startPadding;
        this.endPadding = endPadding;
    }

    @Override
    public char get(long i) {
        i-= startPadding;

        if (i<0 || i> rowSet.size()-1) {
            return NULL_CHAR;
        }

        return columnSource.getPrevChar(rowSet.get(i));
    }

    @Override
    public char getPrev(long offset) {
        return get(offset);
    }

    @Override
    public DbCharArray subArray(long fromIndex, long toIndex) {
        fromIndex-=startPadding;
        toIndex-=startPadding;

        final long realFrom = ClampUtil.clampLong(0, rowSet.size(), fromIndex);
        final long realTo = ClampUtil.clampLong(0, rowSet.size(), toIndex);

        long newStartPadding=toIndex<0 ? toIndex-fromIndex : Math.max(0, -fromIndex);
        long newEndPadding= fromIndex>= rowSet.size() ? toIndex-fromIndex : Math.max(0, toIndex - rowSet.size());

        return new DbPrevCharArrayColumnWrapper(columnSource, rowSet.subSetByPositionRange(realFrom, realTo), newStartPadding, newEndPadding, true);
    }

    @Override
    public DbCharArray subArrayByPositions(long[] positions) {
        RowSetBuilderRandom builder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();

        for (long position : positions) {
            final long realPos = position - startPadding;

            if (realPos < rowSet.size()) {
                builder.addKey(rowSet.get(realPos));
            }
        }

        return new DbPrevCharArrayColumnWrapper(columnSource, builder.build(), 0, 0);
    }

    @Override
    public char[] toArray() {
        return toArray(false,Integer.MAX_VALUE);
    }

    public char[] toArray(boolean shouldBeNullIfOutofBounds,int maxSize) {
        if (shouldBeNullIfOutofBounds && (startPadding>0 || endPadding>0)){
            return null;
        }

        long sz=Math.min(size(),maxSize);

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
