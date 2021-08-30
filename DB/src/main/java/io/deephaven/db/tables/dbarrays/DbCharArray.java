/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.CharChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.libs.primitives.CharacterPrimitives;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.DbPrimitiveArrayType;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public interface DbCharArray extends DbArrayBase<DbCharArray> {

    long serialVersionUID = -1373264425081841175L;

    static DbPrimitiveArrayType<DbCharArray, Character> type() {
        return DbPrimitiveArrayType.of(DbCharArray.class, CharType.instance());
    }

    char get(long i);

    @Override
    DbCharArray subArray(long fromIndex, long toIndex);

    @Override
    DbCharArray subArrayByPositions(long [] positions);

    @Override
    char[] toArray();

    @Override
    long size();

    char getPrev(long i);

    @Override
    @FinalDefault
    default Class getComponentType() {
        return char.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this DbArrayBase that is flattened out to only reference memory.  */
    @Override
    DbCharArray getDirect();

    @Override
    default Chunk<Attributes.Values> toChunk()  {
        return CharChunk.chunkWrap(toArray());
    }

    @Override
    default void fillChunk(WritableChunk destChunk) {
        destChunk.asWritableCharChunk().copyFromTypedArray(toArray(),0, 0, (int)size());
    }

    static String charValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveCharValToString((Character)val);
    }

    static String primitiveCharValToString(final char val) {
        return  CharacterPrimitives.isNull(val) ? NULL_ELEMENT_STRING : Character.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array       The DbCharArray to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final DbCharArray array, final int prefixLength) {
        if (array.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), prefixLength);
        builder.append(primitiveCharValToString(array.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveCharValToString(array.get(ei)));
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
     * @param aArray The LHS of the equality test (always a DbCharArray)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final DbCharArray aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof DbCharArray)) {
            return false;
        }
        final DbCharArray bArray = (DbCharArray) b;
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
     * @param array The DbCharArray to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final DbCharArray array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Character.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" DbCharArray implementations.
     */
    abstract class Indirect implements DbCharArray {

        private static final long serialVersionUID = 1L;

        @Override
        public DbCharArray getDirect() {
            return new DbCharArrayDirect(toArray());
        }

        @Override
        public final String toString() {
            return DbCharArray.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return DbCharArray.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return DbCharArray.hashCode(this);
        }

        protected final Object writeReplace() {
            return new DbCharArrayDirect(toArray());
        }
    }
}
