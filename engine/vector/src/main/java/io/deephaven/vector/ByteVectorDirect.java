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

import static io.deephaven.util.QueryConstants.NULL_BYTE;

@ArrayType(type = byte[].class)
public class ByteVectorDirect implements ByteVector {

    private final static long serialVersionUID = 3636374971797603565L;

    private final byte[] data;

    public ByteVectorDirect(byte... data){
        this.data = data;
    }

    @Override
    public byte get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_BYTE;
        }
        return data[LongSizedDataStructure.intSize("ByteVectorDirect get",  i)];
    }

    @Override
    public ByteVector subVector(long fromIndex, long toIndex) {
        return new ByteVectorSlice(this, fromIndex, toIndex - fromIndex);
    }

    public ByteVector subVectorByPositions(long [] positions) {
        return new ByteSubVector(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public byte[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public ByteVectorDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return ByteVector.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof ByteVectorDirect) {
            return Arrays.equals(data, ((ByteVectorDirect) obj).data);
        }
        return ByteVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return ByteVector.hashCode(this);
    }
}
