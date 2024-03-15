//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.array;

import io.deephaven.qst.type.PrimitiveType;

import java.util.Collection;

/**
 * A primitive array-like object.
 *
 * @param <T> the boxed primitive type
 * @see ByteArray
 * @see BooleanArray
 * @see CharArray
 * @see ShortArray
 * @see IntArray
 * @see LongArray
 * @see FloatArray
 * @see DoubleArray
 */
public interface PrimitiveArray<T> extends Array<T> {

    static <T> ArrayBuilder<T, ? extends PrimitiveArray<T>, ?> builder(PrimitiveType<T> type) {
        return TypeToArrayBuilder.of(type, Util.DEFAULT_BUILDER_INITIAL_CAPACITY);
    }

    static <T> ArrayBuilder<T, ? extends PrimitiveArray<T>, ?> builder(PrimitiveType<T> type,
            int initialCapacity) {
        return TypeToArrayBuilder.of(type, initialCapacity);
    }

    static <T> PrimitiveArray<T> empty(PrimitiveType<T> type) {
        return builder(type, 0).build();
    }

    static <T> PrimitiveArray<T> of(PrimitiveType<T> type, T... data) {
        return builder(type, data.length).add(data).build();
    }

    static <T> PrimitiveArray<T> of(PrimitiveType<T> type, Iterable<T> data) {
        if (data instanceof Collection) {
            return of(type, (Collection<T>) data);
        }
        return builder(type, Util.DEFAULT_BUILDER_INITIAL_CAPACITY).add(data).build();
    }

    static <T> PrimitiveArray<T> of(PrimitiveType<T> type, Collection<T> data) {
        return builder(type, data.size()).add(data).build();
    }

    /**
     * @return the boxed value at {@code index}
     */
    T value(int index);

    /**
     * @return whether the value at {@code index} is {@code null}
     */
    boolean isNull(int index);

    PrimitiveType<T> componentType();

    <R> R walk(Visitor<R> visitor);


    interface Visitor<R> {
        R visit(ByteArray byteArray);

        R visit(BooleanArray booleanArray);

        R visit(CharArray charArray);

        R visit(ShortArray shortArray);

        R visit(IntArray intArray);

        R visit(LongArray longArray);

        R visit(FloatArray floatArray);

        R visit(DoubleArray doubleArray);
    }
}
