/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbCharArray and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.DoubleChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.libs.primitives.DoublePrimitives;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.DbPrimitiveArrayType;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public interface DbDoubleArray extends DbArrayBase<DbDoubleArray> {

    long serialVersionUID = 7218901311693729986L;

    static DbPrimitiveArrayType<DbDoubleArray, Double> type() {
        return DbPrimitiveArrayType.of(DbDoubleArray.class, DoubleType.instance());
    }

    double get(long i);

    @Override
    DbDoubleArray subArray(long fromIndex, long toIndex);

    @Override
    DbDoubleArray subArrayByPositions(long [] positions);

    @Override
    double[] toArray();

    @Override
    long size();

    double getPrev(long i);

    @Override
    @FinalDefault
    default Class getComponentType() {
        return double.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this DbArrayBase that is flattened out to only reference memory.  */
    @Override
    DbDoubleArray getDirect();

    @Override
    default Chunk<Attributes.Values> toChunk()  {
        return DoubleChunk.chunkWrap(toArray());
    }

    @Override
    default void fillChunk(WritableChunk destChunk) {
        destChunk.asWritableDoubleChunk().copyFromTypedArray(toArray(),0, 0, (int)size());
    }

    static String doubleValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveDoubleValToString((Double)val);
    }

    static String primitiveDoubleValToString(final double val) {
        return  DoublePrimitives.isNull(val) ? NULL_ELEMENT_STRING : Double.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array       The DbDoubleArray to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final DbDoubleArray array, final int prefixLength) {
        if (array.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), prefixLength);
        builder.append(primitiveDoubleValToString(array.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveDoubleValToString(array.get(ei)));
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
     * @param aArray The LHS of the equality test (always a DbDoubleArray)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final DbDoubleArray aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof DbDoubleArray)) {
            return false;
        }
        final DbDoubleArray bArray = (DbDoubleArray) b;
        final long size = aArray.size();
        if (size != bArray.size()) {
            return false;
        }
        for (long ei = 0; ei < size; ++ei) {
            // region elementEquals
            if (Double.doubleToLongBits(aArray.get(ei)) != Double.doubleToLongBits(bArray.get(ei))) {
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
     * @param array The DbDoubleArray to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final DbDoubleArray array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Double.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" DbDoubleArray implementations.
     */
    abstract class Indirect implements DbDoubleArray {

        private static final long serialVersionUID = 7218901311693729986L;

        @Override
        public DbDoubleArray getDirect() {
            return new DbDoubleArrayDirect(toArray());
        }

        @Override
        public final String toString() {
            return DbDoubleArray.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return DbDoubleArray.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return DbDoubleArray.hashCode(this);
        }

        protected final Object writeReplace() {
            return new DbDoubleArrayDirect(toArray());
        }
    }
}
