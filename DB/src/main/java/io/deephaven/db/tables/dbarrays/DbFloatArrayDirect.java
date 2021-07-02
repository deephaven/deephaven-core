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

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

@ArrayType(type = float[].class)
public class DbFloatArrayDirect implements DbFloatArray {

    private final static long serialVersionUID = -8263599481663466384L;

    private final float[] data;

    public DbFloatArrayDirect(float... data){
        this.data = data;
    }

    @Override
    public float get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_FLOAT;
        }
        return data[LongSizedDataStructure.intSize("DbArrayDirect get",  i)];
    }

    public float getPrev(long offset) {
        return get(offset);
    }

    @Override
    public DbFloatArray subArray(long fromIndex, long toIndex) {
        return new DbFloatArraySlice(this, fromIndex, toIndex - fromIndex);
    }

    public DbFloatArray subArrayByPositions(long [] positions) {
        return new DbSubFloatArray(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public float[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public DbFloatArrayDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return DbFloatArray.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof DbFloatArrayDirect) {
            return Arrays.equals(data, ((DbFloatArrayDirect) obj).data);
        }
        return DbFloatArray.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return DbFloatArray.hashCode(this);
    }
}
