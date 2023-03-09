/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.vector;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Arrays;

import static io.deephaven.base.ClampUtil.clampLong;
import static io.deephaven.vector.Vector.clampIndex;

/**
 * A subset of an {@link ObjectVector} according to a range of positions.
 */
public class ObjectVectorSlice<COMPONENT_TYPE> extends ObjectVector.Indirect<COMPONENT_TYPE> {

    private final ObjectVector<COMPONENT_TYPE> innerArray;
    private final long offsetIndex;
    private final long length;
    private final long innerArrayValidFromInclusive;
    private final long innerArrayValidToExclusive;

    private ObjectVectorSlice(
            @NotNull final ObjectVector<COMPONENT_TYPE> innerArray,
            final long offsetIndex,
            final long length,
            final long innerArrayValidFromInclusive,
            final long innerArrayValidToExclusive) {
        Assert.geqZero(length, "length");
        Assert.leq(innerArrayValidFromInclusive, "innerArrayValidFromInclusive",
                innerArrayValidToExclusive, "innerArrayValidToExclusive");
        this.innerArray = innerArray;
        this.offsetIndex = offsetIndex;
        this.length = length;
        this.innerArrayValidFromInclusive = innerArrayValidFromInclusive;
        this.innerArrayValidToExclusive = innerArrayValidToExclusive;
    }

    public ObjectVectorSlice(@NotNull final ObjectVector<COMPONENT_TYPE> innerArray, final long offsetIndex,
            final long length) {
        this(innerArray, offsetIndex, length,
                clampLong(0, innerArray.size(), offsetIndex),
                clampLong(0, innerArray.size(), offsetIndex + length));
    }

    @Override
    public COMPONENT_TYPE get(final long index) {
        return innerArray
                .get(clampIndex(innerArrayValidFromInclusive, innerArrayValidToExclusive, index + offsetIndex));
    }

    @Override
    public ObjectVector<COMPONENT_TYPE> subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        final long newLength = toIndexExclusive - fromIndexInclusive;
        final long newOffsetIndex = offsetIndex + fromIndexInclusive;
        return new ObjectVectorSlice<>(innerArray, newOffsetIndex, newLength,
                clampLong(innerArrayValidFromInclusive, innerArrayValidToExclusive, newOffsetIndex),
                clampLong(innerArrayValidFromInclusive, innerArrayValidToExclusive, newOffsetIndex + newLength));
    }

    @Override
    public ObjectVector<COMPONENT_TYPE> subVectorByPositions(final long[] positions) {
        return innerArray.subVectorByPositions(Arrays.stream(positions)
                .map(p -> clampIndex(innerArrayValidFromInclusive, innerArrayValidToExclusive, p + offsetIndex))
                .toArray());
    }

    @Override
    public COMPONENT_TYPE[] toArray() {
        if (innerArray instanceof ObjectVectorDirect
                && offsetIndex >= innerArrayValidFromInclusive
                && offsetIndex + length <= innerArrayValidToExclusive) {
            return Arrays.copyOfRange(innerArray.toArray(),
                    LongSizedDataStructure.intSize("ObjectVectorSlice.toArray", offsetIndex),
                    LongSizedDataStructure.intSize("ObjectVectorSlice.toArray", offsetIndex + length));
        }
        // noinspection unchecked
        final COMPONENT_TYPE[] result = (COMPONENT_TYPE[]) Array.newInstance(getComponentType(),
                LongSizedDataStructure.intSize("ObjectVectorSlice.toArray", length));
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
    public Class<COMPONENT_TYPE> getComponentType() {
        return innerArray.getComponentType();
    }

    @Override
    public boolean isEmpty() {
        return length == 0;
    }
}
