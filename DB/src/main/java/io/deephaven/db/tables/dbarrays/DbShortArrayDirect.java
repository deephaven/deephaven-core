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

import static io.deephaven.util.QueryConstants.NULL_SHORT;

@ArrayType(type = short[].class)
public class DbShortArrayDirect implements DbShortArray {

    private final static long serialVersionUID = -4415134364550246624L;

    private final short[] data;

    public DbShortArrayDirect(short... data){
        this.data = data;
    }

    @Override
    public short get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_SHORT;
        }
        return data[LongSizedDataStructure.intSize("DbArrayDirect get",  i)];
    }

    public short getPrev(long offset) {
        return get(offset);
    }

    @Override
    public DbShortArray subArray(long fromIndex, long toIndex) {
        return new DbShortArraySlice(this, fromIndex, toIndex - fromIndex);
    }

    public DbShortArray subArrayByPositions(long [] positions) {
        return new DbSubShortArray(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public short[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public DbShortArrayDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return DbShortArray.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof DbShortArrayDirect) {
            return Arrays.equals(data, ((DbShortArrayDirect) obj).data);
        }
        return DbShortArray.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return DbShortArray.hashCode(this);
    }
}
