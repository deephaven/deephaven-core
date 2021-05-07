/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbArrayCharWrapper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.tables.utils.ArrayUtils;
import io.deephaven.util.type.TypeUtils;

public class DbArrayIntWrapper extends DbArray.Indirect<Integer> {

    private static final long serialVersionUID = 3710738419894435589L;

    private final DbIntArray innerArray;

    @Override
    public DbArray toDbArray() {
        return this;
    }

    public DbArrayIntWrapper(DbIntArray innerArray) {
        this.innerArray = innerArray;
    }

    @Override
    public Integer get(long i) {
        return TypeUtils.box(innerArray.get(i));
    }

    @Override
    public DbArray<Integer> subArray(long fromIndexInclusive, long toIndexExclusive) {
        return new DbArrayIntWrapper(innerArray.subArray(fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public DbArray<Integer> subArrayByPositions(long[] positions) {
        return new DbArrayIntWrapper(innerArray.subArrayByPositions(positions));
    }

    @Override
    public Integer[] toArray() {
        return ArrayUtils.getBoxedArray(innerArray.toArray());
    }

    @Override
    public long size() {
        return innerArray.size();
    }

    @Override
    public Class<Integer> getComponentType() {
        return Integer.class;
    }

    @Override
    public DbArrayBase getDirect() {
        return new DbIntArrayDirect(innerArray.toArray());
    }

    @Override
    public Integer getPrev(long offset) {
        return get(offset);
    }
}
