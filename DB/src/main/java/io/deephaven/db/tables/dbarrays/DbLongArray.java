/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbCharArray and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.libs.primitives.LongPrimitives;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.DbPrimitiveArrayType;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public interface DbLongArray extends DbArrayBase<DbLongArray> {

    long serialVersionUID = -4934601086974582202L;

    static DbPrimitiveArrayType<DbLongArray, Long> type() {
        return DbPrimitiveArrayType.of(DbLongArray.class, LongType.instance());
    }

    long get(long i);

    @Override
    DbLongArray subArray(long fromIndex, long toIndex);

    @Override
    DbLongArray subArrayByPositions(long [] positions);

    @Override
    long[] toArray();

    @Override
    long size();

    long getPrev(long i);

    @Override
    @FinalDefault
    default Class getComponentType() {
        return long.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this DbArrayBase that is flattened out to only reference memory.  */
    @Override
    DbLongArray getDirect();

    @Override
    default Chunk<Attributes.Values> toChunk()  {
        return LongChunk.chunkWrap(toArray());
    }

    @Override
    default void fillChunk(WritableChunk destChunk) {
        destChunk.asWritableLongChunk().copyFromTypedArray(toArray(),0, 0, (int)size());
    }

    static String longValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveLongValToString((Long)val);
    }

    static String primitiveLongValToString(final long val) {
        return  LongPrimitives.isNull(val) ? NULL_ELEMENT_STRING : Long.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array       The DbLongArray to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final DbLongArray array, final int prefixLength) {
        if (array.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), prefixLength);
        builder.append(primitiveLongValToString(array.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveLongValToString(array.get(ei)));
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
     * @param aArray The LHS of the equality test (always a DbLongArray)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final DbLongArray aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof DbLongArray)) {
            return false;
        }
        final DbLongArray bArray = (DbLongArray) b;
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
     * @param array The DbLongArray to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final DbLongArray array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Long.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" DbLongArray implementations.
     */
    abstract class Indirect implements DbLongArray {

        private static final long serialVersionUID = -4934601086974582202L;

        @Override
        public DbLongArray getDirect() {
            return new DbLongArrayDirect(toArray());
        }

        @Override
        public final String toString() {
            return DbLongArray.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return DbLongArray.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return DbLongArray.hashCode(this);
        }

        protected final Object writeReplace() {
            return new DbLongArrayDirect(toArray());
        }
    }
}
