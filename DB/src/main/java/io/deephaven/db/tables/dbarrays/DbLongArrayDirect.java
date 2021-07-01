/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbCharArrayDirect and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.util.annotations.ArrayType;
import io.deephaven.util.annotations.ArrayTypeGetter;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_LONG;

@ArrayType(type = long[].class)
public class DbLongArrayDirect implements DbLongArray {

    private final static long serialVersionUID = 1233975234000551534L;

    private final long[] data;

    public DbLongArrayDirect(long... data){
        this.data = data;
    }

    @Override
    public long get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_LONG;
        }
        return data[LongSizedDataStructure.intSize("DbArrayDirect get",  i)];
    }

    public long getPrev(long offset) {
        return get(offset);
    }

    @Override
    public DbLongArray subArray(long fromIndex, long toIndex) {
        return new DbLongArraySlice(this, fromIndex, toIndex - fromIndex);
    }

    public DbLongArray subArrayByPositions(long [] positions) {
        return new DbSubLongArray(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public long[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public DbLongArrayDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return DbLongArray.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof DbLongArrayDirect) {
            return Arrays.equals(data, ((DbLongArrayDirect) obj).data);
        }
        return DbLongArray.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return DbLongArray.hashCode(this);
    }
}
