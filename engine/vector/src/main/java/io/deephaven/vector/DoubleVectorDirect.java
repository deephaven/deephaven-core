/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorDirect and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.annotations.ArrayType;
import io.deephaven.util.annotations.ArrayTypeGetter;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

@ArrayType(type = double[].class)
public class DoubleVectorDirect implements DoubleVector {

    private final static long serialVersionUID = 3636374971797603565L;

    private final double[] data;

    public DoubleVectorDirect(double... data){
        this.data = data;
    }

    @Override
    public double get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_DOUBLE;
        }
        return data[LongSizedDataStructure.intSize("DoubleVectorDirect get",  i)];
    }

    @Override
    public DoubleVector subVector(long fromIndex, long toIndex) {
        return new DoubleVectorSlice(this, fromIndex, toIndex - fromIndex);
    }

    public DoubleVector subVectorByPositions(long [] positions) {
        return new DoubleSubVector(this, positions);
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
    public DoubleVectorDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return DoubleVector.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof DoubleVectorDirect) {
            return Arrays.equals(data, ((DoubleVectorDirect) obj).data);
        }
        return DoubleVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return DoubleVector.hashCode(this);
    }
}
