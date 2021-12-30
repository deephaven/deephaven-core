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

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

@ArrayType(type = float[].class)
public class FloatVectorDirect implements FloatVector {

    private final static long serialVersionUID = 3636374971797603565L;

    private final float[] data;

    public FloatVectorDirect(float... data){
        this.data = data;
    }

    @Override
    public float get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_FLOAT;
        }
        return data[LongSizedDataStructure.intSize("FloatVectorDirect get",  i)];
    }

    @Override
    public FloatVector subVector(long fromIndex, long toIndex) {
        return new FloatVectorSlice(this, fromIndex, toIndex - fromIndex);
    }

    public FloatVector subVectorByPositions(long [] positions) {
        return new FloatSubVector(this, positions);
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
    public FloatVectorDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return FloatVector.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof FloatVectorDirect) {
            return Arrays.equals(data, ((FloatVectorDirect) obj).data);
        }
        return FloatVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return FloatVector.hashCode(this);
    }
}
