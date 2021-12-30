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

import static io.deephaven.util.QueryConstants.NULL_SHORT;

@ArrayType(type = short[].class)
public class ShortVectorDirect implements ShortVector {

    private final static long serialVersionUID = 3636374971797603565L;

    private final short[] data;

    public ShortVectorDirect(short... data){
        this.data = data;
    }

    @Override
    public short get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_SHORT;
        }
        return data[LongSizedDataStructure.intSize("ShortVectorDirect get",  i)];
    }

    @Override
    public ShortVector subVector(long fromIndex, long toIndex) {
        return new ShortVectorSlice(this, fromIndex, toIndex - fromIndex);
    }

    public ShortVector subVectorByPositions(long [] positions) {
        return new ShortSubVector(this, positions);
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
    public ShortVectorDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return ShortVector.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof ShortVectorDirect) {
            return Arrays.equals(data, ((ShortVectorDirect) obj).data);
        }
        return ShortVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return ShortVector.hashCode(this);
    }
}
