/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbCharArray and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ByteChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.libs.primitives.BytePrimitives;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.DbPrimitiveArrayType;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public interface DbByteArray extends DbArrayBase<DbByteArray> {

    long serialVersionUID = 8519130615638683196L;

    static DbPrimitiveArrayType<DbByteArray, Byte> type() {
        return DbPrimitiveArrayType.of(DbByteArray.class, ByteType.instance());
    }

    byte get(long i);

    @Override
    DbByteArray subArray(long fromIndex, long toIndex);

    @Override
    DbByteArray subArrayByPositions(long [] positions);

    @Override
    byte[] toArray();

    @Override
    long size();

    byte getPrev(long i);

    @Override
    @FinalDefault
    default Class getComponentType() {
        return byte.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this DbArrayBase that is flattened out to only reference memory.  */
    @Override
    DbByteArray getDirect();

    @Override
    default Chunk<Attributes.Values> toChunk()  {
        return ByteChunk.chunkWrap(toArray());
    }

    @Override
    default void fillChunk(WritableChunk destChunk) {
        destChunk.asWritableByteChunk().copyFromTypedArray(toArray(),0, 0, (int)size());
    }

    static String byteValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveByteValToString((Byte)val);
    }

    static String primitiveByteValToString(final byte val) {
        return  BytePrimitives.isNull(val) ? NULL_ELEMENT_STRING : Byte.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array       The DbByteArray to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final DbByteArray array, final int prefixLength) {
        if (array.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), prefixLength);
        builder.append(primitiveByteValToString(array.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveByteValToString(array.get(ei)));
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
     * @param aArray The LHS of the equality test (always a DbByteArray)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final DbByteArray aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof DbByteArray)) {
            return false;
        }
        final DbByteArray bArray = (DbByteArray) b;
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
     * @param array The DbByteArray to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final DbByteArray array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Byte.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" DbByteArray implementations.
     */
    abstract class Indirect implements DbByteArray {

        private static final long serialVersionUID = 8519130615638683196L;

        @Override
        public DbByteArray getDirect() {
            return new DbByteArrayDirect(toArray());
        }

        @Override
        public final String toString() {
            return DbByteArray.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return DbByteArray.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return DbByteArray.hashCode(this);
        }

        protected final Object writeReplace() {
            return new DbByteArrayDirect(toArray());
        }
    }
}
