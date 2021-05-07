/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbArrayCharWrapper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.tables.utils.ArrayUtils;
import io.deephaven.util.type.TypeUtils;

public class DbArrayShortWrapper extends DbArray.Indirect<Short> {

    private static final long serialVersionUID = -4721120214268745033L;

    private final DbShortArray innerArray;

    @Override
    public DbArray toDbArray() {
        return this;
    }

    public DbArrayShortWrapper(DbShortArray innerArray) {
        this.innerArray = innerArray;
    }

    @Override
    public Short get(long i) {
        return TypeUtils.box(innerArray.get(i));
    }

    @Override
    public DbArray<Short> subArray(long fromIndexInclusive, long toIndexExclusive) {
        return new DbArrayShortWrapper(innerArray.subArray(fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public DbArray<Short> subArrayByPositions(long[] positions) {
        return new DbArrayShortWrapper(innerArray.subArrayByPositions(positions));
    }

    @Override
    public Short[] toArray() {
        return ArrayUtils.getBoxedArray(innerArray.toArray());
    }

    @Override
    public long size() {
        return innerArray.size();
    }

    @Override
    public Class<Short> getComponentType() {
        return Short.class;
    }

    @Override
    public DbArrayBase getDirect() {
        return new DbShortArrayDirect(innerArray.toArray());
    }

    @Override
    public Short getPrev(long offset) {
        return get(offset);
    }
}
