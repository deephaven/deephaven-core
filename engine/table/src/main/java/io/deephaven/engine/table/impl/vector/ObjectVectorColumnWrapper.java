/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.vector;

import io.deephaven.base.ClampUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import io.deephaven.vector.Vector;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;

public class ObjectVectorColumnWrapper<T> extends ObjectVector.Indirect<T> {

    private static final long serialVersionUID = -5944424618636079377L;

    private final ColumnSource<T> columnSource;
    private final RowSet rowSet;
    private final long startPadding;
    private final long endPadding;

    public ObjectVectorColumnWrapper(@NotNull final ColumnSource<T> columnSource, @NotNull final RowSet rowSet) {
        this(columnSource, rowSet, 0, 0);
    }

    public ObjectVectorColumnWrapper(@NotNull final ColumnSource<T> columnSource, @NotNull final RowSet rowSet,
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
    public ObjectVector<T> subVector(long fromIndexInclusive, long toIndexExclusive) {
        fromIndexInclusive -= startPadding;
        toIndexExclusive -= startPadding;

        final long realFrom = ClampUtil.clampLong(0, rowSet.size(), fromIndexInclusive);
        final long realTo = ClampUtil.clampLong(0, rowSet.size(), toIndexExclusive);

        long newStartPadding =
                toIndexExclusive < 0 ? toIndexExclusive - fromIndexInclusive : Math.max(0, -fromIndexInclusive);
        long newEndPadding = fromIndexInclusive >= rowSet.size() ? toIndexExclusive - fromIndexInclusive
                : (int) Math.max(0, toIndexExclusive - rowSet.size());

        return new ObjectVectorColumnWrapper<>(columnSource, rowSet.subSetByPositionRange(realFrom, realTo),
                newStartPadding, newEndPadding);
    }

    public ObjectVector<T> subVectorByPositions(long[] positions) {
        RowSetBuilderRandom builder = RowSetFactory.builderRandom();

        for (long position : positions) {
            final long realPos = position - startPadding;

            if (realPos < rowSet.size()) {
                builder.addKey(rowSet.get(realPos));
            }
        }

        return new ObjectVectorColumnWrapper<>(columnSource, builder.build(), 0, 0);
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
    public ObjectVector<T> getDirect() {
        if (Vector.class.isAssignableFrom(getComponentType())) {
            // recursion!
            final long size = size();
            // noinspection unchecked
            final T[] array =
                    (T[]) Array.newInstance(getComponentType(), LongSizedDataStructure.intSize("getDirect", size));
            for (int ii = 0; ii < size; ++ii) {
                final T element = get(ii);
                if (element == null) {
                    array[ii] = null;
                } else {
                    // noinspection unchecked
                    array[ii] = (T) ((Vector<?>) element).getDirect();
                }
            }
            return new ObjectVectorDirect<>(array);
        } else {
            return new ObjectVectorDirect<>(toArray());
        }
    }
}
