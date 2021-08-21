/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbCharArray and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.IntChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.libs.primitives.IntegerPrimitives;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.DbPrimitiveArrayType;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public interface DbIntArray extends DbArrayBase<DbIntArray> {

    long serialVersionUID = -4282375411744560278L;

    static DbPrimitiveArrayType<DbIntArray, Integer> type() {
        return DbPrimitiveArrayType.of(DbIntArray.class, IntType.instance());
    }

    int get(long i);

    @Override
    DbIntArray subArray(long fromIndex, long toIndex);

    @Override
    DbIntArray subArrayByPositions(long [] positions);

    @Override
    int[] toArray();

    @Override
    long size();

    int getPrev(long i);

    @Override
    @FinalDefault
    default Class getComponentType() {
        return int.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this DbArrayBase that is flattened out to only reference memory.  */
    @Override
    DbIntArray getDirect();

    @Override
    default Chunk<Attributes.Values> toChunk()  {
        return IntChunk.chunkWrap(toArray());
    }

    @Override
    default void fillChunk(WritableChunk destChunk) {
        destChunk.asWritableIntChunk().copyFromTypedArray(toArray(),0, 0, (int)size());
    }

    static String intValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveIntValToString((Integer)val);
    }

    static String primitiveIntValToString(final int val) {
        return  IntegerPrimitives.isNull(val) ? NULL_ELEMENT_STRING : Integer.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array       The DbIntArray to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final DbIntArray array, final int prefixLength) {
        if (array.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), prefixLength);
        builder.append(primitiveIntValToString(array.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveIntValToString(array.get(ei)));
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
     * @param aArray The LHS of the equality test (always a DbIntArray)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final DbIntArray aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof DbIntArray)) {
            return false;
        }
        final DbIntArray bArray = (DbIntArray) b;
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
     * @param array The DbIntArray to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final DbIntArray array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Integer.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" DbIntArray implementations.
     */
    abstract class Indirect implements DbIntArray {

        private static final long serialVersionUID = -4282375411744560278L;

        @Override
        public DbIntArray getDirect() {
            return new DbIntArrayDirect(toArray());
        }

        @Override
        public final String toString() {
            return DbIntArray.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return DbIntArray.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return DbIntArray.hashCode(this);
        }

        protected final Object writeReplace() {
            return new DbIntArrayDirect(toArray());
        }
    }
}
