/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.tables.utils.ArrayUtils;
import io.deephaven.util.type.TypeUtils;

public class DbArrayCharWrapper extends DbArray.Indirect<Character> {

    private static final long serialVersionUID = 4186419447841973418L;

    private final DbCharArray innerArray;

    @Override
    public DbArray toDbArray() {
        return this;
    }

    public DbArrayCharWrapper(DbCharArray innerArray) {
        this.innerArray = innerArray;
    }

    @Override
    public Character get(long i) {
        return TypeUtils.box(innerArray.get(i));
    }

    @Override
    public DbArray<Character> subArray(long fromIndexInclusive, long toIndexExclusive) {
        return new DbArrayCharWrapper(innerArray.subArray(fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public DbArray<Character> subArrayByPositions(long[] positions) {
        return new DbArrayCharWrapper(innerArray.subArrayByPositions(positions));
    }

    @Override
    public Character[] toArray() {
        return ArrayUtils.getBoxedArray(innerArray.toArray());
    }

    @Override
    public long size() {
        return innerArray.size();
    }

    @Override
    public Class<Character> getComponentType() {
        return Character.class;
    }

    @Override
    public DbArrayBase getDirect() {
        return new DbCharArrayDirect(innerArray.toArray());
    }

    @Override
    public Character getPrev(long offset) {
        return get(offset);
    }
}
