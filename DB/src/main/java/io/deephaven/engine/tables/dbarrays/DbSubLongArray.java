/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbSubCharArray and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tables.dbarrays;

import io.deephaven.engine.util.LongSizedDataStructure;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

public class DbSubLongArray extends DbLongArray.Indirect {

    private static final long serialVersionUID = 1L;

    private final DbLongArray innerArray;
    private final long positions[];

    public DbSubLongArray(@NotNull final DbLongArray innerArray, @NotNull final long[] positions) {
        this.innerArray = innerArray;
        this.positions = positions;
    }

    @Override
    public long get(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_LONG;
        }
        return innerArray.get(positions[LongSizedDataStructure.intSize("SubArray get", index)]);
    }

    @Override
    public DbLongArray subArray(final long fromIndex, final long toIndex) {
        return innerArray.subArrayByPositions(DbArrayBase.mapSelectedPositionRange(positions, fromIndex, toIndex));
    }

    @Override
    public DbLongArray subArrayByPositions(final long[] positions) {
        return innerArray.subArrayByPositions(DbArrayBase.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public long[] toArray() {
        final long[] result = new long[positions.length];
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
    public boolean isEmpty() {
        return positions.length == 0;
    }
}
