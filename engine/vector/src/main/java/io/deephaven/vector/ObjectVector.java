/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.qst.type.GenericVectorType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

public interface ObjectVector<T> extends Vector<ObjectVector<T>> {

    long serialVersionUID = 2691131699080413017L;

    static <T> GenericVectorType<ObjectVector, T> type(GenericType<T> genericType) {
        return GenericVectorType.of(ObjectVector.class, genericType);
    }

    T get(long i);

    ObjectVector<T> subVector(long fromIndexInclusive, long toIndexExclusive);

    ObjectVector<T> subVectorByPositions(long[] positions);

    T[] toArray();

    Class<T> getComponentType();

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    ObjectVector<T> getDirect();

    static String defaultValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : val.toString();
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array The Vector to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final ObjectVector<?> array, final int prefixLength) {
        if (array.isEmpty()) {
            return "[]";
        }

        final Function<Object, String> valToString = Vector.classToHelper(array.getComponentType());

        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), prefixLength);
        builder.append(valToString.apply(array.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(valToString.apply(array.get(ei)));
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
     * @param aVector The LHS of the equality test (always a Vector)
     * @param b The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final ObjectVector aVector, @Nullable final Object b) {
        if (aVector == b) {
            return true;
        }
        if (!(b instanceof ObjectVector)) {
            return false;
        }
        final ObjectVector bArray = (ObjectVector) b;
        final long size = aVector.size();
        if (size != bArray.size()) {
            return false;
        }
        for (long ei = 0; ei < size; ++ei) {
            if (!Objects.equals(aVector.get(ei), bArray.get(ei))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in
     * {@link Arrays#hashCode(Object[])}.
     *
     * @param array The Vector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final ObjectVector array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Objects.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" Vector implementations.
     */
    abstract class Indirect<T> implements ObjectVector<T> {

        private static final long serialVersionUID = 1L;

        @Override
        public final String toString() {
            return ObjectVector.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return ObjectVector.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return ObjectVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return new ObjectVectorDirect<>(toArray());
        }
    }
}
