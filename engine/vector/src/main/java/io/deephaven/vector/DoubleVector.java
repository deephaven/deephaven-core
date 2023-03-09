/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVector and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * A {@link Vector} of primitive doubles.
 */
public interface DoubleVector extends Vector<DoubleVector> {

    static PrimitiveVectorType<DoubleVector, Double> type() {
        return PrimitiveVectorType.of(DoubleVector.class, DoubleType.instance());
    }

    double get(long index);

    @Override
    DoubleVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    DoubleVector subVectorByPositions(long[] positions);

    @Override
    double[] toArray();

    @Override
    long size();

    @Override
    @FinalDefault
    default Class<?> getComponentType() {
        return double.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    /** Return a version of this Vector that is flattened out to only reference memory. */
    @Override
    DoubleVector getDirect();

    static String doubleValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveDoubleValToString((Double) val);
    }

    static String primitiveDoubleValToString(final double val) {
        return val == QueryConstants.NULL_DOUBLE ? NULL_ELEMENT_STRING : Double.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The DoubleVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final DoubleVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        builder.append(primitiveDoubleValToString(vector.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(primitiveDoubleValToString(vector.get(ei)));
        }
        if (displaySize == vector.size()) {
            builder.append(']');
        } else {
            builder.append(", ...]");
        }
        return builder.toString();
    }

    /**
     * Helper method for implementing {@link Object#equals(Object)}.
     *
     * @param aVector The LHS of the equality test (always a DoubleVector)
     * @param bObj The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final DoubleVector aVector, @Nullable final Object bObj) {
        if (aVector == bObj) {
            return true;
        }
        if (!(bObj instanceof DoubleVector)) {
            return false;
        }
        final DoubleVector bVector = (DoubleVector) bObj;
        final long size = aVector.size();
        if (size != bVector.size()) {
            return false;
        }
        for (long ei = 0; ei < size; ++ei) {
            // region elementEquals
            if (aVector.get(ei) != bVector.get(ei)) {
                return false;
            }
            // endregion elementEquals
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern
     * in {@link Arrays#hashCode(double[])}.
     *
     * @param vector The DoubleVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final DoubleVector vector) {
        final long size = vector.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Double.hashCode(vector.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" DoubleVector implementations.
     */
    abstract class Indirect implements DoubleVector {

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
    }
}
