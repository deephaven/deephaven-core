//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.primitive.value.iterator.ValueIterator;
import io.deephaven.qst.type.GenericVectorType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

/**
 * A {@link Vector} of {@link Object objects}.
 */
public interface ObjectVector<COMPONENT_TYPE> extends Vector<ObjectVector<COMPONENT_TYPE>>, Iterable<COMPONENT_TYPE> {

    long serialVersionUID = 2691131699080413017L;

    static <T> GenericVectorType<ObjectVector<T>, T> type(GenericType<T> genericType) {
        // noinspection unchecked
        return GenericVectorType.of((Class<ObjectVector<T>>) (Class<?>) ObjectVector.class, genericType);
    }

    /**
     * Get the element of this ObjectVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return {@code null}.
     *
     * @param index An offset into this ObjectVector
     * @return The element at the specified offset, or {@code null}
     */
    COMPONENT_TYPE get(long index);

    @Override
    ObjectVector<COMPONENT_TYPE> subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    ObjectVector<COMPONENT_TYPE> subVectorByPositions(long[] positions);

    @Override
    COMPONENT_TYPE[] toArray();

    @Override
    COMPONENT_TYPE[] copyToArray();

    @Override
    ObjectVector<COMPONENT_TYPE> getDirect();

    @Override
    Class<COMPONENT_TYPE> getComponentType();

    @Override
    @FinalDefault
    default ValueIterator<COMPONENT_TYPE> iterator() {
        return iterator(0, size());
    }

    /**
     * Returns an iterator over a slice of this ObjectVector, with equivalent semantics to
     * {@code subVector(fromIndexInclusive, toIndexExclusive).iterator()}.
     *
     * @param fromIndexInclusive The first position to include
     * @param toIndexExclusive The first position after {@code fromIndexInclusive} to not include
     * @return An iterator over the requested slice
     */
    default ValueIterator<COMPONENT_TYPE> iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        return new ValueIterator<>() {

            long nextIndex = fromIndexInclusive;

            @Override
            public COMPONENT_TYPE next() {
                return get(nextIndex++);
            }

            @Override
            public boolean hasNext() {
                return nextIndex < toIndexExclusive;
            }

            @Override
            public long remaining() {
                return toIndexExclusive - nextIndex;
            }
        };
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

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
        try (final CloseableIterator<?> iterator = vector.iterator(0, displaySize)) {
            builder.append(valToString.apply(iterator.next()));
            iterator.forEachRemaining((final Object value) -> builder.append(',').append(valToString.apply(value)));
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
        if (size == 0) {
            return true;
        }
        // @formatter:off
        try (final CloseableIterator<?> aIterator = aVector.iterator();
             final CloseableIterator<?> bIterator = bVector.iterator()) {
            // @formatter:on
            while (aIterator.hasNext()) {
                if (!Objects.deepEquals(aIterator.next(), bIterator.next())) {
                    return false;
                }
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
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseableIterator<?> iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + Objects.hashCode(iterator.next());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" ObjectVector implementations.
     */
    abstract class Indirect<COMPONENT_TYPE> implements ObjectVector<COMPONENT_TYPE> {

        @Override
        public COMPONENT_TYPE[] toArray() {
            final int size = intSize("ObjectVector.toArray");
            // noinspection unchecked
            final COMPONENT_TYPE[] result = (COMPONENT_TYPE[]) Array.newInstance(getComponentType(), size);
            try (final CloseableIterator<COMPONENT_TYPE> iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.next();
                }
            }
            return result;
        }

        @Override
        public COMPONENT_TYPE[] copyToArray() {
            return toArray();
        }

        @Override
        public ObjectVector<COMPONENT_TYPE> getDirect() {
            if (Vector.class.isAssignableFrom(getComponentType())) {
                final int size = LongSizedDataStructure.intSize("ObjectVector.Indirect.getDirect", size());
                // noinspection unchecked
                final COMPONENT_TYPE[] array = (COMPONENT_TYPE[]) Array.newInstance(getComponentType(), size);
                try (final CloseableIterator<COMPONENT_TYPE> iterator = iterator()) {
                    for (int ei = 0; ei < size; ++ei) {
                        final COMPONENT_TYPE element = iterator.next();
                        if (element == null) {
                            array[ei] = null;
                        } else {
                            // noinspection unchecked
                            array[ei] = (COMPONENT_TYPE) ((Vector<?>) element).getDirect();
                        }
                    }
                }
                return new ObjectVectorDirect<>(array);
            } else {
                return new ObjectVectorDirect<>(toArray());
            }
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

        protected final Object writeReplace() {
            // Rather than call getDirect(), allow component Vectors to serialize themselves appropriately
            return new ObjectVectorDirect<>(toArray());
        }
    }
}
