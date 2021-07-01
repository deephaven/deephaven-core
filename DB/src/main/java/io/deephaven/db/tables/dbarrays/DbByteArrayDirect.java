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

import static io.deephaven.util.QueryConstants.NULL_BYTE;

@ArrayType(type = byte[].class)
public class DbByteArrayDirect implements DbByteArray {

    private final static long serialVersionUID = 5978679490703697461L;

    private final byte[] data;

    public DbByteArrayDirect(byte... data){
        this.data = data;
    }

    @Override
    public byte get(long i) {
        if (i < 0 || i > data.length - 1) {
            return NULL_BYTE;
        }
        return data[LongSizedDataStructure.intSize("DbArrayDirect get",  i)];
    }

    public byte getPrev(long offset) {
        return get(offset);
    }

    @Override
    public DbByteArray subArray(long fromIndex, long toIndex) {
        return new DbByteArraySlice(this, fromIndex, toIndex - fromIndex);
    }

    public DbByteArray subArrayByPositions(long [] positions) {
        return new DbSubByteArray(this, positions);
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
    public DbByteArrayDirect getDirect() {
        return this;
    }

    @Override
    public final String toString() {
        return DbByteArray.toString(this, 10);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof DbByteArrayDirect) {
            return Arrays.equals(data, ((DbByteArrayDirect) obj).data);
        }
        return DbByteArray.equals(this, obj);
    }

    @Override
    public final int hashCode() {
        return DbByteArray.hashCode(this);
    }
}
