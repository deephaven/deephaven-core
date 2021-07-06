/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.util.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

public class DbSubBooleanArray extends DbBooleanArrayDirect.Indirect {

    private static final long serialVersionUID = 1L;

    private final DbBooleanArray innerArray;
    private final long positions[];

    public DbSubBooleanArray(@NotNull final DbBooleanArray innerArray, @NotNull final long[] positions) {
        this.innerArray = innerArray;
        this.positions = positions;
    }

    @Override
    public Boolean get(final long index) {
        if (index < 0 || index >= positions.length) {
            return null;
        }
        return innerArray.get(positions[LongSizedDataStructure.intSize("DbSubBooleanArray get",  index)]);
    }

    @Override
    public DbBooleanArray subArray(final long fromIndex, final long toIndex) {
        return innerArray.subArrayByPositions(DbArrayBase.mapSelectedPositionRange(positions, fromIndex, toIndex));
    }

    @Override
    public DbBooleanArray subArrayByPositions(final long[] positions) {
        return innerArray.subArrayByPositions(DbArrayBase.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public Boolean[] toArray() {
        final Boolean[] result = new Boolean[positions.length];
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

    @Override
    public DbBooleanArrayDirect getDirect() {
        return new DbBooleanArrayDirect(toArray());
    }
}
