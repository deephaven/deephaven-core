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

import static io.deephaven.util.QueryConstants.NULL_INT;

@ArrayType(type = int[].class)
public class DbIntArrayDirect implements DbIntArray {

    private final static long serialVersionUID = -7790095389322728763L;

    private final int[] data;

    public DbIntArrayDirect(int... data){
        this.data = data;
    }

    @Override
    public int get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_INT;
        }
        return data[LongSizedDataStructure.intSize("DbArrayDirect get",  i)];
    }

    public int getPrev(long offset) {
        return get(offset);
    }

    @Override
    public DbIntArray subArray(long fromIndex, long toIndex) {
        return new DbIntArraySlice(this, fromIndex, toIndex - fromIndex);
    }

    public DbIntArray subArrayByPositions(long [] positions) {
        return new DbSubIntArray(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public int[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public DbIntArrayDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return DbIntArray.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof DbIntArrayDirect) {
            return Arrays.equals(data, ((DbIntArrayDirect) obj).data);
        }
        return DbIntArray.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return DbIntArray.hashCode(this);
    }
}
