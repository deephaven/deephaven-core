/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
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

/**
 * A {@link Vector} of {@link Object objects}.
 */
public interface ObjectVector<COMPONENT_TYPE> extends Vector<ObjectVector<COMPONENT_TYPE>> {

    static <T> GenericVectorType<ObjectVector<T>, T> type(GenericType<T> genericType) {
        // noinspection unchecked
        return GenericVectorType.of((Class<ObjectVector<T>>) (Class<?>) ObjectVector.class, genericType);
    }

    COMPONENT_TYPE get(long index);

    ObjectVector<COMPONENT_TYPE> subVector(long fromIndexInclusive, long toIndexExclusive);

    ObjectVector<COMPONENT_TYPE> subVectorByPositions(long[] positions);

    COMPONENT_TYPE[] toArray();

    Class<COMPONENT_TYPE> getComponentType();

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    ObjectVector<COMPONENT_TYPE> getDirect();

    static String defaultValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : val.toString();
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The ObjectVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final ObjectVector<?> vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }

        final Function<Object, String> valToString = Vector.classToHelper(vector.getComponentType());

        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        builder.append(valToString.apply(vector.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(valToString.apply(vector.get(ei)));
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
     * @param aVector The LHS of the equality test (always an ObjectVector)
     * @param bObj The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final ObjectVector<?> aVector, @Nullable final Object bObj) {
        if (aVector == bObj) {
            return true;
        }
        if (!(bObj instanceof ObjectVector)) {
            return false;
        }
        final ObjectVector<?> bVector = (ObjectVector<?>) bObj;
        final long size = aVector.size();
        if (size != bVector.size()) {
            return false;
        }
        for (long ei = 0; ei < size; ++ei) {
            if (!Objects.equals(aVector.get(ei), bVector.get(ei))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in
     * {@link Arrays#hashCode(Object[])}.
     *
     * @param vector The ObjectVector to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final ObjectVector<?> vector) {
        final long size = vector.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Objects.hashCode(vector.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" ObjectVector implementations.
     */
    abstract class Indirect<COMPONENT_TYPE> implements ObjectVector<COMPONENT_TYPE> {

        @Override
        public ObjectVector<COMPONENT_TYPE> getDirect() {
            return new ObjectVectorDirect<>(toArray());
        }

        @Override
        public final String toString() {
            return ObjectVector.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(final Object obj) {
            return ObjectVector.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return ObjectVector.hashCode(this);
        }
    }
}
