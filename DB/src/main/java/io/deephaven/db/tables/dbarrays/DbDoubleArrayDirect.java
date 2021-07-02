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

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

@ArrayType(type = double[].class)
public class DbDoubleArrayDirect implements DbDoubleArray {

    private final static long serialVersionUID = 3262776153086160765L;

    private final double[] data;

    public DbDoubleArrayDirect(double... data){
        this.data = data;
    }

    @Override
    public double get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_DOUBLE;
        }
        return data[LongSizedDataStructure.intSize("DbArrayDirect get",  i)];
    }

    public double getPrev(long offset) {
        return get(offset);
    }

    @Override
    public DbDoubleArray subArray(long fromIndex, long toIndex) {
        return new DbDoubleArraySlice(this, fromIndex, toIndex - fromIndex);
    }

    public DbDoubleArray subArrayByPositions(long [] positions) {
        return new DbSubDoubleArray(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public double[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public DbDoubleArrayDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return DbDoubleArray.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof DbDoubleArrayDirect) {
            return Arrays.equals(data, ((DbDoubleArrayDirect) obj).data);
        }
        return DbDoubleArray.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return DbDoubleArray.hashCode(this);
    }
}
