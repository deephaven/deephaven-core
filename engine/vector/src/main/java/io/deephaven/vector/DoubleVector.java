/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVector and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public interface DoubleVector extends Vector<DoubleVector> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<DoubleVector, Double> type() {
        return PrimitiveVectorType.of(DoubleVector.class, DoubleType.instance());
    }

    double get(long i);

    @Override
    DoubleVector subVector(long fromIndex, long toIndex);

    @Override
    DoubleVector subVectorByPositions(long [] positions);

    @Override
    double[] toArray();

    @Override
    long size();

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

    /** Return a version of this Vector that is flattened out to only reference memory.  */
    @Override
    DoubleVector getDirect();

    static String doubleValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveDoubleValToString((Double)val);
    }

    static String primitiveDoubleValToString(final double val) {
        return val == QueryConstants.NULL_DOUBLE ? NULL_ELEMENT_STRING : Double.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array       The DoubleVector to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final DoubleVector array, final int prefixLength) {
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
     * @param aArray The LHS of the equality test (always a DoubleVector)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final DoubleVector aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof DoubleVector)) {
            return false;
        }
        final DoubleVector bArray = (DoubleVector) b;
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
     * @param array The DoubleVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final DoubleVector array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Double.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" DoubleVector implementations.
     */
    abstract class Indirect implements DoubleVector {

        private static final long serialVersionUID = 1L;

        @Override
        public DoubleVector getDirect() {
            return new DoubleVectorDirect(toArray());
        }

        @Override
        public final String toString() {
            return DoubleVector.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return DoubleVector.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return DoubleVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return new DoubleVectorDirect(toArray());
        }
    }
}
