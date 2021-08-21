/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbCharArray and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.FloatChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.libs.primitives.FloatPrimitives;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.DbPrimitiveArrayType;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public interface DbFloatArray extends DbArrayBase<DbFloatArray> {

    long serialVersionUID = -1889118072737983807L;

    static DbPrimitiveArrayType<DbFloatArray, Float> type() {
        return DbPrimitiveArrayType.of(DbFloatArray.class, FloatType.instance());
    }

    float get(long i);

    @Override
    DbFloatArray subArray(long fromIndex, long toIndex);

    @Override
    DbFloatArray subArrayByPositions(long [] positions);

    @Override
    float[] toArray();

    @Override
    long size();

    float getPrev(long i);

    @Override
    @FinalDefault
    default Class getComponentType() {
        return float.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this DbArrayBase that is flattened out to only reference memory.  */
    @Override
    DbFloatArray getDirect();

    @Override
    default Chunk<Attributes.Values> toChunk()  {
        return FloatChunk.chunkWrap(toArray());
    }

    @Override
    default void fillChunk(WritableChunk destChunk) {
        destChunk.asWritableFloatChunk().copyFromTypedArray(toArray(),0, 0, (int)size());
    }

    static String floatValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveFloatValToString((Float)val);
    }

    static String primitiveFloatValToString(final float val) {
        return  FloatPrimitives.isNull(val) ? NULL_ELEMENT_STRING : Float.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array       The DbFloatArray to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final DbFloatArray array, final int prefixLength) {
        if (array.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), prefixLength);
        builder.append(primitiveFloatValToString(array.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveFloatValToString(array.get(ei)));
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
     * @param aArray The LHS of the equality test (always a DbFloatArray)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final DbFloatArray aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof DbFloatArray)) {
            return false;
        }
        final DbFloatArray bArray = (DbFloatArray) b;
        final long size = aArray.size();
        if (size != bArray.size()) {
            return false;
        }
        for (long ei = 0; ei < size; ++ei) {
            // region elementEquals
            if (Float.floatToIntBits(aArray.get(ei)) != Float.floatToIntBits(bArray.get(ei))) {
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
     * @param array The DbFloatArray to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final DbFloatArray array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Float.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" DbFloatArray implementations.
     */
    abstract class Indirect implements DbFloatArray {

        private static final long serialVersionUID = -1889118072737983807L;

        @Override
        public DbFloatArray getDirect() {
            return new DbFloatArrayDirect(toArray());
        }

        @Override
        public final String toString() {
            return DbFloatArray.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return DbFloatArray.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return DbFloatArray.hashCode(this);
        }

        protected final Object writeReplace() {
            return new DbFloatArrayDirect(toArray());
        }
    }
}
