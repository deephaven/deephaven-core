/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

@Deprecated
public interface BooleanVector extends Vector<BooleanVector> {

    static PrimitiveVectorType<BooleanVector, Boolean> type() {
        return PrimitiveVectorType.of(BooleanVector.class, BooleanType.instance());
    }

    Boolean get(long i);
    BooleanVector subVector(long fromIndex, long toIndex);
    BooleanVector subVectorByPositions(long [] positions);
    Boolean[] toArray();

    @Override
    @FinalDefault
    default Class getComponentType() {
        return Boolean.class;
    }

    /** Return a version of this Vector that is flattened out to only reference memory.  */
    BooleanVectorDirect getDirect();

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array The BooleanVector to convert to a String
     * @return The String representation of array
     */
    static String toString(@NotNull final BooleanVector array) {
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
     * @param aArray The LHS of the equality test (always a BooleanVector)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final BooleanVector aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof BooleanVector)) {
            return false;
        }
        final BooleanVector bArray = (BooleanVector) b;
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
     * @param array The BooleanVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final BooleanVector array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Objects.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" BooleanVector implementations.
     */
    @Deprecated
    abstract class Indirect implements BooleanVector {

        private static final long serialVersionUID = 1L;

        @Override
        public final String toString() {
            return BooleanVector.toString(this);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return BooleanVector.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return BooleanVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return new BooleanVectorDirect(toArray());
        }
    }
}
