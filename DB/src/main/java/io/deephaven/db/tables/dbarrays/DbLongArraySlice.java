/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbCharArraySlice and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.base.ClampUtil.clampLong;
import static io.deephaven.db.tables.dbarrays.DbArrayBase.clampIndex;

public class DbLongArraySlice extends DbLongArray.Indirect {

    private static final long serialVersionUID = 1L;

    private final DbLongArray innerArray;
    private final long offsetIndex;
    private final long length;
    private final long innerArrayValidFromInclusive;
    private final long innerArrayValidToExclusive;

    public DbLongArraySlice(@NotNull final DbLongArray innerArray, final long offsetIndex, final long length, final long innerArrayValidFromInclusive, final long innerArrayValidToExclusive) {
        Assert.geqZero(length, "length");
        Assert.leq(innerArrayValidFromInclusive, "innerArrayValidFromInclusive", innerArrayValidToExclusive, "innerArrayValidToExclusive");
        this.innerArray = innerArray;
        this.offsetIndex = offsetIndex;
        this.length = length;
        this.innerArrayValidFromInclusive = innerArrayValidFromInclusive;
        this.innerArrayValidToExclusive = innerArrayValidToExclusive;
    }

    public DbLongArraySlice(@NotNull final DbLongArray innerArray, final long offsetIndex, final long length) {
        this(innerArray, offsetIndex, length,
                clampLong(0, innerArray.size(), offsetIndex),
                clampLong(0, innerArray.size(), offsetIndex + length));
    }

    @Override
    public long get(final long index) {
        return innerArray.get(clampIndex(innerArrayValidFromInclusive, innerArrayValidToExclusive, index + offsetIndex));
    }

    @Override
    public DbLongArray subArray(final long fromIndexInclusive, final long toIndexExclusive) {
            Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
            final long newLength = toIndexExclusive - fromIndexInclusive;
            final long newOffsetIndex = offsetIndex + fromIndexInclusive;
            return new DbLongArraySlice(innerArray, newOffsetIndex, newLength,
                    clampLong(innerArrayValidFromInclusive, innerArrayValidToExclusive, newOffsetIndex),
                    clampLong(innerArrayValidFromInclusive, innerArrayValidToExclusive, newOffsetIndex + newLength));
        }

    @Override
    public DbLongArray subArrayByPositions(final long[] positions) {
        return innerArray.subArrayByPositions(Arrays.stream(positions).map(p -> clampIndex(innerArrayValidFromInclusive, innerArrayValidToExclusive, p + offsetIndex)).toArray());
    }

    @Override
    public long[] toArray() {
        if (innerArray instanceof DbLongArrayDirect && offsetIndex >= innerArrayValidFromInclusive && offsetIndex + length <= innerArrayValidToExclusive) {
            return Arrays.copyOfRange(innerArray.toArray(), LongSizedDataStructure.intSize("toArray", offsetIndex), LongSizedDataStructure.intSize("toArray", offsetIndex + length));
        }
        final long[] result = new long[LongSizedDataStructure.intSize("toArray", length)];
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
    public long getPrev(final long index) {
        if (index < 0 || index >= length) {
            return QueryConstants.NULL_LONG;
        }
        return innerArray.getPrev(offsetIndex + index);
    }

    @Override
    public boolean isEmpty() {
        return length == 0;
    }
}
