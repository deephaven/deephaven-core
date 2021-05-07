/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

public class DbArrayBooleanWrapper extends DbArray.Indirect<Boolean> {

    private final DbBooleanArray innerArray;

    public DbArrayBooleanWrapper(DbBooleanArray innerArray) {
        this.innerArray = innerArray;
    }

    public Boolean get(long i) {
        return innerArray.get(i);
    }

    @Override
    public DbArray subArray(long fromIndexInclusive, long toIndexExclusive) {
        return new DbArrayBooleanWrapper(innerArray.subArray(fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public DbArray subArrayByPositions(long [] positions) {
        return new DbArrayBooleanWrapper(innerArray.subArrayByPositions(positions));
    }

    @Override
    public Boolean[] toArray() {
        return innerArray.toArray();
    }

    @Override
    public long size() {
        return innerArray.size();
    }

    @Override
    public Class getComponentType() {
        return Boolean.class;
    }

    @Override
    public DbBooleanArrayDirect getDirect() {
        return new DbBooleanArrayDirect(innerArray.toArray());
    }

    @Override
    public Boolean getPrev(long offset) {
        return get(offset);
    }

    @Override
    public DbArray toDbArray() {
        return this;
    }
}
