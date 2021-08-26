/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.util.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;

public class DbSubArray<T> extends DbArray.Indirect<T> {

    private static final long serialVersionUID = 1L;

    private final DbArray<T> innerArray;
    private final long positions[];

    public DbSubArray(@NotNull final DbArray<T> innerArray, @NotNull final long[] positions) {
        this.innerArray = innerArray;
        this.positions = positions;
    }

    @Override
    public T get(final long index) {
        if (index < 0 || index >= positions.length) {
            return null;
        }
        return innerArray.get(positions[LongSizedDataStructure.intSize("subarray array access", index)]);
    }

    @Override
    public DbArray<T> subArray(final long fromIndexInclusive, final long toIndexExclusive) {
        return innerArray.subArrayByPositions(
                DbArrayBase.mapSelectedPositionRange(positions, fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public DbArray<T> subArrayByPositions(final long[] positions) {
        return innerArray.subArrayByPositions(DbArrayBase.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public T[] toArray() {
        // noinspection unchecked
        final T[] result = (T[]) Array.newInstance(getComponentType(), positions.length);
        for (int ii = 0; ii < positions.length; ++ii) {
            result[ii] = get(ii);
        }
        return result;
    }

    @Override
    public long size() {
        return positions.length;
    }

    @Override
    public Class<T> getComponentType() {
        return innerArray.getComponentType();
    }

    @Override
    public T getPrev(final long index) {
        if (index < 0 || index >= positions.length) {
            return null;
        }
        return innerArray.getPrev(positions[LongSizedDataStructure.intSize("DbSubArray getPrev", index)]);
    }

    @Override
    public boolean isEmpty() {
        return positions.length == 0;
    }

    @Override
    public DbArray<T> getDirect() {
        return new DbArrayDirect<>(toArray());
    }
}
