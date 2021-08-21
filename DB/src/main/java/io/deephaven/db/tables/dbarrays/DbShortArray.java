/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbCharArray and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ShortChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.libs.primitives.ShortPrimitives;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.DbPrimitiveArrayType;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public interface DbShortArray extends DbArrayBase<DbShortArray> {

    long serialVersionUID = -6562228894877343013L;

    static DbPrimitiveArrayType<DbShortArray, Short> type() {
        return DbPrimitiveArrayType.of(DbShortArray.class, ShortType.instance());
    }

    short get(long i);

    @Override
    DbShortArray subArray(long fromIndex, long toIndex);

    @Override
    DbShortArray subArrayByPositions(long [] positions);

    @Override
    short[] toArray();

    @Override
    long size();

    short getPrev(long i);

    @Override
    @FinalDefault
    default Class getComponentType() {
        return short.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this DbArrayBase that is flattened out to only reference memory.  */
    @Override
    DbShortArray getDirect();

    @Override
    default Chunk<Attributes.Values> toChunk()  {
        return ShortChunk.chunkWrap(toArray());
    }

    @Override
    default void fillChunk(WritableChunk destChunk) {
        destChunk.asWritableShortChunk().copyFromTypedArray(toArray(),0, 0, (int)size());
    }

    static String shortValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveShortValToString((Short)val);
    }

    static String primitiveShortValToString(final short val) {
        return  ShortPrimitives.isNull(val) ? NULL_ELEMENT_STRING : Short.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array       The DbShortArray to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final DbShortArray array, final int prefixLength) {
        if (array.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), prefixLength);
        builder.append(primitiveShortValToString(array.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveShortValToString(array.get(ei)));
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
     * @param aArray The LHS of the equality test (always a DbShortArray)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final DbShortArray aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof DbShortArray)) {
            return false;
        }
        final DbShortArray bArray = (DbShortArray) b;
        final long size = aArray.size();
        if (size != bArray.size()) {
            return false;
        }
        for (long ei = 0; ei < size; ++ei) {
            // region elementEquals
            if (aArray.get(ei) != bArray.get(ei)) {
                return false;
            }
            // endregion elementEquals
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in
     * {@link Arrays#hashCode(Object[])}.
     *
     * @param array The DbShortArray to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final DbShortArray array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Short.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" DbShortArray implementations.
     */
    abstract class Indirect implements DbShortArray {

        private static final long serialVersionUID = -6562228894877343013L;

        @Override
        public DbShortArray getDirect() {
            return new DbShortArrayDirect(toArray());
        }

        @Override
        public final String toString() {
            return DbShortArray.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return DbShortArray.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return DbShortArray.hashCode(this);
        }

        protected final Object writeReplace() {
            return new DbShortArrayDirect(toArray());
        }
    }
}
