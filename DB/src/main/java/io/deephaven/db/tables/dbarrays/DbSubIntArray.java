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

public class DbSubIntArray extends DbIntArray.Indirect {

    private static final long serialVersionUID = 1L;

    private final DbIntArray innerArray;
    private final long positions[];

    public DbSubIntArray(@NotNull final DbIntArray innerArray, @NotNull final long[] positions) {
        this.innerArray = innerArray;
        this.positions = positions;
    }

    @Override
    public int get(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_INT;
        }
        return innerArray.get(positions[LongSizedDataStructure.intSize("SubArray get", index)]);
    }

    @Override
    public DbIntArray subArray(final long fromIndex, final long toIndex) {
        return innerArray.subArrayByPositions(DbArrayBase.mapSelectedPositionRange(positions, fromIndex, toIndex));
    }

    @Override
    public DbIntArray subArrayByPositions(final long[] positions) {
        return innerArray.subArrayByPositions(DbArrayBase.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public int[] toArray() {
        //noinspection unchecked
        final int[] result = new int[positions.length];
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
    public int getPrev(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_INT;
        }
        return innerArray.getPrev(positions[LongSizedDataStructure.intSize("getPrev", index)]);
    }

    @Override
    public boolean isEmpty() {
        return positions.length == 0;
    }
}
