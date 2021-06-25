/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.util.annotations.ArrayType;
import io.deephaven.util.annotations.ArrayTypeGetter;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

@ArrayType(type = char[].class)
public class DbCharArrayDirect implements DbCharArray {

    private final static long serialVersionUID = 3636374971797603565L;

    private final char[] data;

    public DbCharArrayDirect(char... data){
        this.data = data;
    }

    @Override
    public char get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_CHAR;
        }
        return data[LongSizedDataStructure.intSize("DbArrayDirect get",  i)];
    }

    public char getPrev(long offset) {
        return get(offset);
    }

    @Override
    public DbCharArray subArray(long fromIndex, long toIndex) {
        return new DbCharArraySlice(this, fromIndex, toIndex - fromIndex);
    }

    public DbCharArray subArrayByPositions(long [] positions) {
        return new DbSubCharArray(this, positions);
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
    public DbCharArrayDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return DbCharArray.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof DbCharArrayDirect) {
            return Arrays.equals(data, ((DbCharArrayDirect) obj).data);
        }
        return DbCharArray.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return DbCharArray.hashCode(this);
    }
}
