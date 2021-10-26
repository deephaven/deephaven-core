/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.dbarrays;

import io.deephaven.base.ClampUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.util.LongSizedDataStructure;
import io.deephaven.engine.tables.dbarrays.DbArray;
import io.deephaven.engine.tables.dbarrays.DbArrayDirect;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.utils.RowSet;
import io.deephaven.engine.v2.utils.RowSetFactoryImpl;
import io.deephaven.engine.v2.utils.RowSetBuilderRandom;
import io.deephaven.engine.v2.utils.TrackingRowSet;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;

public class DbPrevArrayColumnWrapper<T> extends DbArray.Indirect<T> {

    private static final long serialVersionUID = -5944424618636079377L;

    private final ColumnSource<T> columnSource;
    private final RowSet rowSet;
    private final long startPadding;
    private final long endPadding;

    public DbPrevArrayColumnWrapper(@NotNull final ColumnSource<T> columnSource, @NotNull final TrackingRowSet rowSet) {
        this(columnSource, rowSet, 0, 0);
    }

    public DbPrevArrayColumnWrapper(@NotNull final ColumnSource<T> columnSource, @NotNull final TrackingRowSet rowSet,
            final long startPadding, final long endPadding) {
        this(columnSource, rowSet, startPadding, endPadding, false);
    }

    private DbPrevArrayColumnWrapper(@NotNull final ColumnSource<T> columnSource, @NotNull final TrackingRowSet rowSet,
            final long startPadding, final long endPadding, final boolean alreadyPrevIndex) {
        Assert.neqNull(rowSet, "rowSet");
        this.columnSource = columnSource;
        this.rowSet = alreadyPrevIndex ? rowSet : rowSet.getPrevRowSet();
        this.startPadding = startPadding;
        this.endPadding = endPadding;
    }

    @Override
    public T get(long i) {
        i -= startPadding;

        if (i < 0 || i > rowSet.size() - 1) {
            return null;
        }
        return columnSource.getPrev(rowSet.get(i));
    }

    @Override
    public DbArray<T> subArray(long fromIndexInclusive, long toIndexExclusive) {
        fromIndexInclusive -= startPadding;
        toIndexExclusive -= startPadding;

        final long realFrom = ClampUtil.clampLong(0, rowSet.size(), fromIndexInclusive);
        final long realTo = ClampUtil.clampLong(0, rowSet.size(), toIndexExclusive);

        long newStartPadding =
                toIndexExclusive < 0 ? toIndexExclusive - fromIndexInclusive : Math.max(0, -fromIndexInclusive);
        long newEndPadding = fromIndexInclusive >= rowSet.size() ? toIndexExclusive - fromIndexInclusive
                : (int) Math.max(0, toIndexExclusive - rowSet.size());

        return new DbPrevArrayColumnWrapper<>(columnSource, rowSet.subSetByPositionRange(realFrom, realTo), newStartPadding,
                newEndPadding, true);
    }

    @Override
    public DbArray<T> subArrayByPositions(long[] positions) {
        RowSetBuilderRandom builder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();

        for (long position : positions) {
            final long realPos = position - startPadding;

            if (realPos < rowSet.size()) {
                builder.addKey(rowSet.get(realPos));
            }
        }

        return new DbPrevArrayColumnWrapper<>(columnSource, builder.build(), 0, 0);
    }

    @Override
    public T[] toArray() {
        return toArray(false, Integer.MAX_VALUE);
    }

    public T[] toArray(boolean shouldBeNullIfOutofBounds, int maxSize) {
        if (shouldBeNullIfOutofBounds && (startPadding > 0 || endPadding > 0)) {
            return null;
        }

        long sz = Math.min(size(), maxSize);

        @SuppressWarnings("unchecked")
        T result[] = (T[]) Array.newInstance(TypeUtils.getBoxedType(columnSource.getType()),
                LongSizedDataStructure.intSize("toArray", sz));
        for (int i = 0; i < sz; i++) {
            result[i] = get(i);
        }

        return result;
    }

    @Override
    public long size() {
        return startPadding + rowSet.size() + endPadding;
    }

    @Override
    public Class<T> getComponentType() {
        return columnSource.getType();
    }

    @Override
    public DbArray<T> getDirect() {
        return new DbArrayDirect<>(toArray());
    }

    @Override
    public T getPrev(long offset) {
        return get(offset);
    }
}
