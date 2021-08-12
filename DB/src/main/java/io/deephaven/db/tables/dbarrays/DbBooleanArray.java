/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.DbPrimitiveArrayType;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

@Deprecated
public interface DbBooleanArray extends DbArrayBase {

    static DbPrimitiveArrayType<DbBooleanArray, Boolean> type() {
        return DbPrimitiveArrayType.of(DbBooleanArray.class, BooleanType.instance());
    }

    Boolean get(long i);
    DbBooleanArray subArray(long fromIndex, long toIndex);
    DbBooleanArray subArrayByPositions(long [] positions);
    Boolean[] toArray();

    @Override
    @FinalDefault
    default Class getComponentType() {
        return Boolean.class;
    }

    /** Return a version of this DbArrayBase that is flattened out to only reference memory.  */
    DbBooleanArrayDirect getDirect();

    @Override
    default Chunk<Attributes.Values> toChunk()  {
        return ObjectChunk.chunkWrap(toArray());
    }

    @Override
    default void fillChunk(WritableChunk destChunk) {
        destChunk.asWritableObjectChunk().copyFromTypedArray(toArray(),0, destChunk.size(), (int)size());
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array The DbBooleanArray to convert to a String
     * @return The String representation of array
     */
    static String toString(@NotNull final DbBooleanArray array) {
        if (array.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), 10);
        builder.append(array.get(0));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(array.get(ei));
        }
        if (displaySize == array.size()) {
            builder.append(']');
        } else {
            builder.append(", ...]");
        }
        return builder.toString();
    }

    /**
     * Helper method for implementing {@link Object#equals(Object)}.
     *
     * @param aArray The LHS of the equality test (always a DbBooleanArray)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final DbBooleanArray aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof DbBooleanArray)) {
            return false;
        }
        final DbBooleanArray bArray = (DbBooleanArray) b;
        final long size = aArray.size();
        if (size != bArray.size()) {
            return false;
        }
        for (long ei = 0; ei < size; ++ei) {
            if (!Objects.equals(aArray.get(ei), bArray.get(ei))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in
     * {@link Arrays#hashCode(Object[])}.
     *
     * @param array The DbBooleanArray to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final DbBooleanArray array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Objects.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" DbBooleanArray implementations.
     */
    abstract class Indirect implements DbBooleanArray {

        private static final long serialVersionUID = 1L;

        @Override
        public final String toString() {
            return DbBooleanArray.toString(this);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return DbBooleanArray.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return DbBooleanArray.hashCode(this);
        }

        protected final Object writeReplace() {
            return new DbBooleanArrayDirect(toArray());
        }
    }
}
