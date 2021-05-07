/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbArrayCharWrapper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.tables.utils.ArrayUtils;
import io.deephaven.util.type.TypeUtils;

public class DbArrayDoubleWrapper extends DbArray.Indirect<Double> {

    private static final long serialVersionUID = 6570401045990711791L;

    private final DbDoubleArray innerArray;

    @Override
    public DbArray toDbArray() {
        return this;
    }

    public DbArrayDoubleWrapper(DbDoubleArray innerArray) {
        this.innerArray = innerArray;
    }

    @Override
    public Double get(long i) {
        return TypeUtils.box(innerArray.get(i));
    }

    @Override
    public DbArray<Double> subArray(long fromIndexInclusive, long toIndexExclusive) {
        return new DbArrayDoubleWrapper(innerArray.subArray(fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public DbArray<Double> subArrayByPositions(long[] positions) {
        return new DbArrayDoubleWrapper(innerArray.subArrayByPositions(positions));
    }

    @Override
    public Double[] toArray() {
        return ArrayUtils.getBoxedArray(innerArray.toArray());
    }

    @Override
    public long size() {
        return innerArray.size();
    }

    @Override
    public Class<Double> getComponentType() {
        return Double.class;
    }

    @Override
    public DbArrayBase getDirect() {
        return new DbDoubleArrayDirect(innerArray.toArray());
    }

    @Override
    public Double getPrev(long offset) {
        return get(offset);
    }
}
