/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.base.verify.Assert;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.LongStream;

public interface Vector<VECTOR_TYPE extends Vector> extends Serializable, LongSizedDataStructure {
    long serialVersionUID = -2429677814745466454L;

    String NULL_ELEMENT_STRING = " ";

    VECTOR_TYPE subVector(long fromIndex, long toIndex);

    VECTOR_TYPE subVectorByPositions(long[] positions);

    Object toArray();

    Class getComponentType();

    default String toString(int prefixLength) {
        return "";
    }

    default boolean isEmpty() {
        return size() == 0;
    }

    /** Return a version of this Vector that is flattened out to only reference memory. */
    VECTOR_TYPE getDirect();

    static long clampIndex(final long validFromInclusive, final long validToExclusive, final long index) {
        return index < validFromInclusive || index >= validToExclusive ? -1 : index;
    }

    static long[] mapSelectedPositionRange(@NotNull final long[] currentPositions,
            final long selectedRangeStartInclusive, final long selectedRangeEndExclusive) {
        Assert.leq(selectedRangeStartInclusive, "selectedRangeStartInclusive", selectedRangeEndExclusive,
                "selectedRangeEndExclusive");
        return LongStream.range(selectedRangeStartInclusive, selectedRangeEndExclusive)
                .map(s -> s < 0 || s >= currentPositions.length ? -1
                        : currentPositions[LongSizedDataStructure.intSize("mapSelectedPositionRange", s)])
                .toArray();
    }

    static long[] mapSelectedPositions(@NotNull final long[] currentPositions,
            @NotNull final long[] selectedPositions) {
        return Arrays.stream(selectedPositions).map(s -> s < 0 || s >= currentPositions.length ? -1
                : currentPositions[LongSizedDataStructure.intSize("mapSelectedPositions", s)]).toArray();
    }

    static Function<Object, String> classToHelper(final Class clazz) {
        if (clazz.equals(byte.class) || clazz.equals(Byte.class)) {
            return ByteVector::byteValToString;
        } else if (clazz.equals(char.class) || clazz.equals(Character.class)) {
            return CharVector::charValToString;
        } else if (clazz.equals(double.class) || clazz.equals(Double.class)) {
            return DoubleVector::doubleValToString;
        } else if (clazz.equals(float.class) || clazz.equals(Float.class)) {
            return FloatVector::floatValToString;
        } else if (clazz.equals(int.class) || clazz.equals(Integer.class)) {
            return IntVector::intValToString;
        } else if (clazz.equals(long.class) || clazz.equals(Long.class)) {
            return LongVector::longValToString;
        } else if (clazz.equals(short.class) || clazz.equals(Short.class)) {
            return ShortVector::shortValToString;
        } else {
            return ObjectVector::defaultValToString;
        }
    }
}
