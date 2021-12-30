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

import static io.deephaven.util.QueryConstants.NULL_LONG;

@ArrayType(type = long[].class)
public class LongVectorDirect implements LongVector {

    private final static long serialVersionUID = 3636374971797603565L;

    private final long[] data;

    public LongVectorDirect(long... data){
        this.data = data;
    }

    @Override
    public long get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_LONG;
        }
        return data[LongSizedDataStructure.intSize("LongVectorDirect get",  i)];
    }

    @Override
    public LongVector subVector(long fromIndex, long toIndex) {
        return new LongVectorSlice(this, fromIndex, toIndex - fromIndex);
    }

    public LongVector subVectorByPositions(long [] positions) {
        return new LongSubVector(this, positions);
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
    public LongVectorDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return LongVector.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof LongVectorDirect) {
            return Arrays.equals(data, ((LongVectorDirect) obj).data);
        }
        return LongVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return LongVector.hashCode(this);
    }
}
