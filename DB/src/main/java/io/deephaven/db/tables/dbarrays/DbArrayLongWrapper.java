/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbArrayCharWrapper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.tables.utils.ArrayUtils;
import io.deephaven.util.type.TypeUtils;

public class DbArrayLongWrapper extends DbArray.Indirect<Long> {

    private static final long serialVersionUID = -7803689205873236396L;

    private final DbLongArray innerArray;

    @Override
    public DbArray toDbArray() {
        return this;
    }

    public DbArrayLongWrapper(DbLongArray innerArray) {
        this.innerArray = innerArray;
    }

    @Override
    public Long get(long i) {
        return TypeUtils.box(innerArray.get(i));
    }

    @Override
    public DbArray<Long> subArray(long fromIndexInclusive, long toIndexExclusive) {
        return new DbArrayLongWrapper(innerArray.subArray(fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public DbArray<Long> subArrayByPositions(long[] positions) {
        return new DbArrayLongWrapper(innerArray.subArrayByPositions(positions));
    }

    @Override
    public Long[] toArray() {
        return ArrayUtils.getBoxedArray(innerArray.toArray());
    }

    @Override
    public long size() {
        return innerArray.size();
    }

    @Override
    public Class<Long> getComponentType() {
        return Long.class;
    }

    @Override
    public DbArrayBase getDirect() {
        return new DbLongArrayDirect(innerArray.toArray());
    }

    @Override
    public Long getPrev(long offset) {
        return get(offset);
    }
}
