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

    PrimitiveType<T> componentType();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ByteArray byteArray);

        void visit(BooleanArray booleanArray);

        void visit(CharArray charArray);

        void visit(ShortArray shortArray);

        void visit(IntArray intArray);

        void visit(LongArray longArray);

        void visit(FloatArray floatArray);

        void visit(DoubleArray doubleArray);
    }
}
