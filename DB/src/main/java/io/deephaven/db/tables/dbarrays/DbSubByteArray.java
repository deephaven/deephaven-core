/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbSubCharArray and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

public class DbSubByteArray extends DbByteArray.Indirect {

    private static final long serialVersionUID = 1L;

    private final DbByteArray innerArray;
    private final long positions[];

    public DbSubByteArray(@NotNull final DbByteArray innerArray, @NotNull final long[] positions) {
        this.innerArray = innerArray;
        this.positions = positions;
    }

    @Override
    public byte get(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_BYTE;
        }
        return innerArray.get(positions[LongSizedDataStructure.intSize("SubArray get", index)]);
    }

    @Override
    public DbByteArray subArray(final long fromIndex, final long toIndex) {
        return innerArray.subArrayByPositions(DbArrayBase.mapSelectedPositionRange(positions, fromIndex, toIndex));
    }

    @Override
    public DbByteArray subArrayByPositions(final long[] positions) {
        return innerArray.subArrayByPositions(DbArrayBase.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public byte[] toArray() {
        //noinspection unchecked
        final byte[] result = new byte[positions.length];
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
    public byte getPrev(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_BYTE;
        }
        return innerArray.getPrev(positions[LongSizedDataStructure.intSize("getPrev", index)]);
    }

    @Override
    public boolean isEmpty() {
        return positions.length == 0;
    }
}
