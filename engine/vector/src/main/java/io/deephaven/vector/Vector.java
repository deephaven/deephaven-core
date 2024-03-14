//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.vector;

import io.deephaven.base.verify.Assert;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.LongStream;

/**
 * Logical data structure representing an ordered list of elements.
 */
public interface Vector<VECTOR_TYPE extends Vector<VECTOR_TYPE>> extends Serializable, LongSizedDataStructure {

    long serialVersionUID = -2429677814745466454L;

    String NULL_ELEMENT_STRING = " ";

    /**
     * Get a Vector that represents a slice of this Vector.
     *
     * @param fromIndexInclusive The first offset into this Vector to include in the result; if negative, the result
     *        will have a range of null values at offsets in {@code [0, -fromIndexInclusive)}
     * @param toIndexExclusive The first offset into this Vector to not include in the result; if larger than
     *        {@code size()}, the result will have a range of null values at the corresponding offsets
     * @return The sub-Vector specified by {@code [fromIndexInclusive, toIndexExclusive)}
     */
    VECTOR_TYPE subVector(long fromIndexInclusive, long toIndexExclusive);

    /**
     * Get a Vector that represents a set of offset positions in this Vector.
     *
     * @param positions The offsets to include; if not within {@code [0, size())}, the corresponding offset in the
     *        result will contain the appropriate null value
     * @return The sub-Vector specified by {@code positions}
     */
    VECTOR_TYPE subVectorByPositions(long[] positions);

    /**
     * Get an array representation of the elements of this Vector. Callers <em>must not</em> mutate the result, as
     * implementations may choose to return their backing array in some cases.
     *
     * @return An array representation of the elements of this Vector that must not be mutated
     */
    Object toArray();

    /**
     * Get an array representation of the elements of this Vector. Callers <em>may freely</em> mutate the result, as it
     * is guaranteed to be freshly-allocated and belongs to the caller upon return.
     *
     * @return An array representation of the elements of this Vector that may be freely mutated
     */
    Object copyToArray();

    /**
     * @return A version of this Vector that is flattened out to only reference memory
     */
    VECTOR_TYPE getDirect();

    /**
     * @return The type of elements contained by this Vector
     */
    Class<?> getComponentType();

    /**
     * Get a String representation of a prefix of this Vector.
     *
     * @param prefixLength The number of elements to include
     * @return The specified prefix String representation
     */
    String toString(int prefixLength);

    /**
     * @return Whether this Vector is empty
     */
    default boolean isEmpty() {
        return size() == 0;
    }

    static long clampIndex(final long validFromInclusive, final long validToExclusive, final long index) {
        return index < validFromInclusive || index >= validToExclusive ? -1 : index;
    }

    static long[] mapSelectedPositionRange(
            @NotNull final long[] currentPositions,
            final long selectedRangeStartInclusive,
            final long selectedRangeEndExclusive) {
        Assert.leq(selectedRangeStartInclusive, "selectedRangeStartInclusive",
                selectedRangeEndExclusive, "selectedRangeEndExclusive");
        return LongStream.range(selectedRangeStartInclusive, selectedRangeEndExclusive)
                .map((final long selected) -> selected < 0 || selected >= currentPositions.length
                        ? -1
                        : currentPositions[(int) selected])
                .toArray();
    }

    static long[] mapSelectedPositions(
            @NotNull final long[] currentPositions,
            @NotNull final long[] selectedPositions) {
        return Arrays.stream(selectedPositions)
                .map((final long selected) -> selected < 0 || selected >= currentPositions.length
                        ? -1
                        : currentPositions[(int) selected])
                .toArray();
    }

    static Function<Object, String> classToHelper(final Class<?> clazz) {
        if (clazz == byte.class || clazz == Byte.class) {
            return ByteVector::byteValToString;
        } else if (clazz == char.class || clazz == Character.class) {
            return CharVector::charValToString;
        } else if (clazz == double.class || clazz == Double.class) {
            return DoubleVector::doubleValToString;
        } else if (clazz == float.class || clazz == Float.class) {
            return FloatVector::floatValToString;
        } else if (clazz == int.class || clazz == Integer.class) {
            return IntVector::intValToString;
        } else if (clazz == long.class || clazz == Long.class) {
            return LongVector::longValToString;
        } else if (clazz == short.class || clazz == Short.class) {
            return ShortVector::shortValToString;
        } else {
            return ObjectVector::defaultValToString;
        }
    }
}
