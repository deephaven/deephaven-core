/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.dbarrays;

import io.deephaven.base.ClampUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.util.LongSizedDataStructure;
import io.deephaven.engine.tables.dbarrays.DbArray;
import io.deephaven.engine.tables.dbarrays.DbArrayBase;
import io.deephaven.engine.tables.dbarrays.DbArrayDirect;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.utils.RowSetFactoryImpl;
import io.deephaven.engine.v2.utils.RowSetBuilderRandom;
import io.deephaven.engine.v2.utils.TrackingRowSet;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;

public class DbArrayColumnWrapper<T> extends DbArray.Indirect<T> {

    private static final long serialVersionUID = -5944424618636079377L;

    private final ColumnSource<T> columnSource;
    private final TrackingRowSet rowSet;
    private final long startPadding;
    private final long endPadding;

    public DbArrayColumnWrapper(@NotNull final ColumnSource<T> columnSource, @NotNull final TrackingRowSet rowSet) {
        this(columnSource, rowSet, 0, 0);
    }

    public DbArrayColumnWrapper(@NotNull final ColumnSource<T> columnSource, @NotNull final TrackingRowSet rowSet,
            final long startPadding, final long endPadding) {
        Assert.neqNull(rowSet, "rowSet");
        this.columnSource = columnSource;
        this.rowSet = rowSet;
        this.startPadding = startPadding;
        this.endPadding = endPadding;
    }

    @Override
    public T get(long i) {
        i -= startPadding;

        if (i < 0 || i > rowSet.size() - 1) {
            return null;
        }

        return columnSource.get(rowSet.get(i));
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

        return new DbArrayColumnWrapper<>(columnSource, rowSet.subSetByPositionRange(realFrom, realTo), newStartPadding,
                newEndPadding);
    }

    public DbArray<T> subArrayByPositions(long[] positions) {
        RowSetBuilderRandom builder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();

        for (long position : positions) {
            final long realPos = position - startPadding;

            if (realPos < rowSet.size()) {
                builder.addKey(rowSet.get(realPos));
            }
        }

        return new DbArrayColumnWrapper<>(columnSource, builder.build(), 0, 0);
    }

    @Override
    public T[] toArray() {
        return toArray(false, Long.MAX_VALUE);
    }

    public T[] toArray(boolean shouldBeNullIfOutofBounds, long maxSize) {
        if (shouldBeNullIfOutofBounds && (startPadding > 0 || endPadding > 0)) {
            return null;
        }

        final long sz = Math.min(size(), maxSize);

        @SuppressWarnings("unchecked")
        final T result[] = (T[]) Array.newInstance(TypeUtils.getBoxedType(columnSource.getType()),
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
        if (DbArrayBase.class.isAssignableFrom(getComponentType())) {
            // recursion!
            final long size = size();
            // noinspection unchecked
            final T[] array =
                    (T[]) Array.newInstance(getComponentType(), LongSizedDataStructure.intSize("toArray", size));
            for (int ii = 0; ii < size; ++ii) {
                final T arrayBase = get(ii);
                if (arrayBase == null) {
                    array[ii] = null;
                } else {
                    // noinspection unchecked
                    array[ii] = (T) ((DbArrayBase<?>) arrayBase).getDirect();
                }
            }
            return new DbArrayDirect<>(array);
        } else {
            return new DbArrayDirect<>(toArray());
        }
    }

    @Override
    public T getPrev(long i) {
        i -= startPadding;

        if (i < 0 || i > rowSet.size() - 1) {
            return null;
        }

        return columnSource.getPrev(rowSet.getPrev(i));
    }
}
