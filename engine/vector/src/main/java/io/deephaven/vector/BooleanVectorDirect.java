/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.util.datastructures.LongSizedDataStructure;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_BOOLEAN;

@Deprecated
public class BooleanVectorDirect implements BooleanVector {

    private final Boolean[] data;

    public BooleanVectorDirect(Boolean... data){
        this.data = data;
    }

    @Override
    public Boolean get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_BOOLEAN;
        }
        return data[LongSizedDataStructure.intSize("BooleanVectorDirect get", i)];
    }

    @Override
    public BooleanVector subVector(long fromIndex, long toIndex) {
        return new BooleanVectorSlice(this, fromIndex, toIndex - fromIndex);
    }

    @Override
    public BooleanVector subVectorByPositions(long [] positions) {
        return new BooleanSubVector(this, positions);
    }

    @Override
    public Boolean[] toArray() {
        return data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public BooleanVectorDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return BooleanVector.toString(this);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof BooleanVectorDirect) {
            return Arrays.equals(data, ((BooleanVectorDirect) obj).data);
        }
        return BooleanVector.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return BooleanVector.hashCode(this);
    }
}
