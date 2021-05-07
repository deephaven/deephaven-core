/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbArrayCharWrapper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.tables.utils.ArrayUtils;
import io.deephaven.util.type.TypeUtils;

public class DbArrayFloatWrapper extends DbArray.Indirect<Float> {

    private static final long serialVersionUID = -161104677395357585L;

    private final DbFloatArray innerArray;

    @Override
    public DbArray toDbArray() {
        return this;
    }

    public DbArrayFloatWrapper(DbFloatArray innerArray) {
        this.innerArray = innerArray;
    }

    @Override
    public Float get(long i) {
        return TypeUtils.box(innerArray.get(i));
    }

    @Override
    public DbArray<Float> subArray(long fromIndexInclusive, long toIndexExclusive) {
        return new DbArrayFloatWrapper(innerArray.subArray(fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public DbArray<Float> subArrayByPositions(long[] positions) {
        return new DbArrayFloatWrapper(innerArray.subArrayByPositions(positions));
    }

    @Override
    public Float[] toArray() {
        return ArrayUtils.getBoxedArray(innerArray.toArray());
    }

    @Override
    public long size() {
        return innerArray.size();
    }

    @Override
    public Class<Float> getComponentType() {
        return Float.class;
    }

    @Override
    public DbArrayBase getDirect() {
        return new DbFloatArrayDirect(innerArray.toArray());
    }

    @Override
    public Float getPrev(long offset) {
        return get(offset);
    }
}
