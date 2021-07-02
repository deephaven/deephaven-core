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

public class DbSubDoubleArray extends DbDoubleArray.Indirect {

    private static final long serialVersionUID = 1L;

    private final DbDoubleArray innerArray;
    private final long positions[];

    public DbSubDoubleArray(@NotNull final DbDoubleArray innerArray, @NotNull final long[] positions) {
        this.innerArray = innerArray;
        this.positions = positions;
    }

    @Override
    public double get(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_DOUBLE;
        }
        return innerArray.get(positions[LongSizedDataStructure.intSize("SubArray get", index)]);
    }

    @Override
    public DbDoubleArray subArray(final long fromIndex, final long toIndex) {
        return innerArray.subArrayByPositions(DbArrayBase.mapSelectedPositionRange(positions, fromIndex, toIndex));
    }

    @Override
    public DbDoubleArray subArrayByPositions(final long[] positions) {
        return innerArray.subArrayByPositions(DbArrayBase.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public double[] toArray() {
        //noinspection unchecked
        final double[] result = new double[positions.length];
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
    public double getPrev(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_DOUBLE;
        }
        return innerArray.getPrev(positions[LongSizedDataStructure.intSize("getPrev", index)]);
    }

    @Override
    public boolean isEmpty() {
        return positions.length == 0;
    }
}
