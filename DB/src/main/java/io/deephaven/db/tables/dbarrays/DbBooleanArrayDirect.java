/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.util.LongSizedDataStructure;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_BOOLEAN;

public class DbBooleanArrayDirect implements DbBooleanArray {

    private final Boolean[] data;

    public DbBooleanArrayDirect(Boolean... data){
        this.data = data;
    }

    @Override
    public Boolean get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_BOOLEAN;
        }
        return data[LongSizedDataStructure.intSize("DbBooleanArrayDirect get", i)];
    }

    @Override
    public DbBooleanArray subArray(long fromIndex, long toIndex) {
        return new DbBooleanArraySlice(this, fromIndex, toIndex - fromIndex);
    }

    @Override
    public DbBooleanArray subArrayByPositions(long [] positions) {
        return new DbSubBooleanArray(this, positions);
    }

    @Override
    public Boolean[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public DbBooleanArrayDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return DbBooleanArray.toString(this);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof DbBooleanArrayDirect) {
            return Arrays.equals(data, ((DbBooleanArrayDirect) obj).data);
        }
        return DbBooleanArray.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return DbBooleanArray.hashCode(this);
    }
}
