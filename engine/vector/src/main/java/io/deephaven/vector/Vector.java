/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.vector;

import io.deephaven.base.verify.Assert;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.LongStream;

/**
 * Logical data structure representing an ordered list of elements.
 */
public interface Vector<VECTOR_TYPE extends Vector<VECTOR_TYPE>> extends LongSizedDataStructure {

    String NULL_ELEMENT_STRING = " ";

    VECTOR_TYPE subVector(long fromIndexInclusive, long toIndexExclusive);

    VECTOR_TYPE subVectorByPositions(long[] positions);

    Object toArray();

    Class<?> getComponentType();

    String toString(int prefixLength);

    default boolean isEmpty() {
        return size() == 0;
    }

    /** Return a version of this Vector that is flattened out to only reference memory. */
    VECTOR_TYPE getDirect();

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
                .map(s -> s < 0 || s >= currentPositions.length
                        ? -1
                        : currentPositions[LongSizedDataStructure.intSize("mapSelectedPositionRange", s)])
                .toArray();
    }

    static long[] mapSelectedPositions(
            @NotNull final long[] currentPositions,
            @NotNull final long[] selectedPositions) {
        return Arrays.stream(selectedPositions).map(s -> s < 0 || s >= currentPositions.length ? -1
                : currentPositions[LongSizedDataStructure.intSize("mapSelectedPositions", s)]).toArray();
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
