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

import static io.deephaven.util.QueryConstants.NULL_INT;

@ArrayType(type = int[].class)
public class IntVectorDirect implements IntVector {

    private final static long serialVersionUID = 3636374971797603565L;

    private final int[] data;

    public IntVectorDirect(int... data){
        this.data = data;
    }

    @Override
    public int get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_INT;
        }
        return data[LongSizedDataStructure.intSize("IntVectorDirect get",  i)];
    }

    @Override
    public IntVector subVector(long fromIndex, long toIndex) {
        return new IntVectorSlice(this, fromIndex, toIndex - fromIndex);
    }

    public IntVector subVectorByPositions(long [] positions) {
        return new IntSubVector(this, positions);
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
    public IntVectorDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return IntVector.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof IntVectorDirect) {
            return Arrays.equals(data, ((IntVectorDirect) obj).data);
        }
        return IntVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return IntVector.hashCode(this);
    }
}
