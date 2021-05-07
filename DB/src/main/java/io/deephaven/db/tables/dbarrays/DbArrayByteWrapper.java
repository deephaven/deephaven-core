/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbArrayCharWrapper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.tables.utils.ArrayUtils;
import io.deephaven.util.type.TypeUtils;

public class DbArrayByteWrapper extends DbArray.Indirect<Byte> {

    private static final long serialVersionUID = -7617549435342988215L;

    private final DbByteArray innerArray;

    @Override
    public DbArray toDbArray() {
        return this;
    }

    public DbArrayByteWrapper(DbByteArray innerArray) {
        this.innerArray = innerArray;
    }

    @Override
    public Byte get(long i) {
        return TypeUtils.box(innerArray.get(i));
    }

    @Override
    public DbArray<Byte> subArray(long fromIndexInclusive, long toIndexExclusive) {
        return new DbArrayByteWrapper(innerArray.subArray(fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public DbArray<Byte> subArrayByPositions(long[] positions) {
        return new DbArrayByteWrapper(innerArray.subArrayByPositions(positions));
    }

    @Override
    public Byte[] toArray() {
        return ArrayUtils.getBoxedArray(innerArray.toArray());
    }

    @Override
    public long size() {
        return innerArray.size();
    }

    @Override
    public Class<Byte> getComponentType() {
        return Byte.class;
    }

    @Override
    public DbArrayBase getDirect() {
        return new DbByteArrayDirect(innerArray.toArray());
    }

    @Override
    public Byte getPrev(long offset) {
        return get(offset);
    }
}
