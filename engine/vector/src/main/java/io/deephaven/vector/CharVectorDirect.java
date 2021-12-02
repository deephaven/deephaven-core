/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.annotations.ArrayType;
import io.deephaven.util.annotations.ArrayTypeGetter;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

@ArrayType(type = char[].class)
public class CharVectorDirect implements CharVector {

    private final static long serialVersionUID = 3636374971797603565L;

    private final char[] data;

    public CharVectorDirect(char... data){
        this.data = data;
    }

    @Override
    public char get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_CHAR;
        }
        return data[LongSizedDataStructure.intSize("CharVectorDirect get",  i)];
    }

    @Override
    public CharVector subVector(long fromIndex, long toIndex) {
        return new CharVectorSlice(this, fromIndex, toIndex - fromIndex);
    }

    public CharVector subVectorByPositions(long [] positions) {
        return new CharSubVector(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public char[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public CharVectorDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return CharVector.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof CharVectorDirect) {
            return Arrays.equals(data, ((CharVectorDirect) obj).data);
        }
        return CharVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return CharVector.hashCode(this);
    }
}
