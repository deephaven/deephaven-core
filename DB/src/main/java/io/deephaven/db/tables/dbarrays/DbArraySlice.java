/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.db.util.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Arrays;

import static io.deephaven.base.ClampUtil.clampLong;
import static io.deephaven.db.tables.dbarrays.DbArrayBase.clampIndex;

public class DbArraySlice<T> extends DbArray.Indirect<T> {

    private static final long serialVersionUID = 1L;

    private final DbArray<T> innerArray;
    private final long offsetIndex;
    private final long length;
    private final long innerArrayValidFromInclusive;
    private final long innerArrayValidToExclusive;

    private DbArraySlice(@NotNull final DbArray<T> innerArray, final long offsetIndex, final long length, final long innerArrayValidFromInclusive, final long innerArrayValidToExclusive) {
        Assert.geqZero(length, "length");
        Assert.leq(innerArrayValidFromInclusive, "innerArrayValidFromInclusive", innerArrayValidToExclusive, "innerArrayValidToExclusive");
        this.innerArray = innerArray;
        this.offsetIndex = offsetIndex;
        this.length = length;
        this.innerArrayValidFromInclusive = innerArrayValidFromInclusive;
        this.innerArrayValidToExclusive = innerArrayValidToExclusive;
    }

    public DbArraySlice(@NotNull final DbArray<T> innerArray, final long offsetIndex, final long length) {
        this(innerArray, offsetIndex, length,
                clampLong(0, innerArray.size(), offsetIndex),
                clampLong(0, innerArray.size(), offsetIndex + length));
    }

    @Override
    public T get(final long index) {
        return innerArray.get(clampIndex(innerArrayValidFromInclusive, innerArrayValidToExclusive, index + offsetIndex));
    }

    @Override
    public DbArray<T> subArray(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        final long newLength = toIndexExclusive - fromIndexInclusive;
        final long newOffsetIndex = offsetIndex + fromIndexInclusive;
        return new DbArraySlice<>(innerArray, newOffsetIndex, newLength,
                clampLong(innerArrayValidFromInclusive, innerArrayValidToExclusive, newOffsetIndex),
                clampLong(innerArrayValidFromInclusive, innerArrayValidToExclusive, newOffsetIndex + newLength));
    }

    @Override
    public DbArray<T> subArrayByPositions(final long[] positions) {
        return innerArray.subArrayByPositions(Arrays.stream(positions).map(p -> clampIndex(innerArrayValidFromInclusive, innerArrayValidToExclusive, p + offsetIndex)).toArray());
    }

    @Override
    public T[] toArray() {
        if (innerArray instanceof DbArrayDirect && offsetIndex >= innerArrayValidFromInclusive && offsetIndex + length <= innerArrayValidToExclusive) {
            return Arrays.copyOfRange(innerArray.toArray(), LongSizedDataStructure.intSize("toArray", offsetIndex), LongSizedDataStructure.intSize("toArray", offsetIndex + length));
        }
        //noinspection unchecked
        final T[] result = (T[])Array.newInstance(getComponentType(), LongSizedDataStructure.intSize("toArray", length));
        for (int ii = 0; ii < length; ++ii) {
            result[ii] = get(ii);
        }
        return result;
    }

    @Override
    public long size() {
        return length;
    }

    @Override
    public Class<T> getComponentType() {
        return innerArray.getComponentType();
    }

    @Override
    public T getPrev(final long index) {
        if (index < 0 || index >= length) {
            return null;
        }
        return innerArray.getPrev(offsetIndex + index);
    }

    @Override
    public boolean isEmpty() {
        return length == 0;
    }

    @Override
    public DbArray<T> getDirect() {
        return new DbArrayDirect<>(toArray());
    }
}
