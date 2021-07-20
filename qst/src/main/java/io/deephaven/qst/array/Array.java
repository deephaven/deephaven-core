package io.deephaven.qst.array;

import io.deephaven.qst.type.Type;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * Provides strong typing around an array-like object.
 *
 * @param <T> the generic type
 * @see PrimitiveArray
 * @see GenericArray
 */
public interface Array<T> extends Iterable<T> {

    static <T> ArrayBuilder<T, ?, ?> builder(Type<T> type) {
        return TypeToArrayBuilder.of(type, 16);
    }

    static <T> ArrayBuilder<T, ?, ?> builder(Type<T> type, int initialCapacity) {
        return TypeToArrayBuilder.of(type, initialCapacity);
    }

    static <T> Array<T> empty(Type<T> type) {
        return builder(type, 0).build();
    }

    static <T> Array<T> of(Type<T> type, T... data) {
        return builder(type, data.length).add(data).build();
    }

    static <T> Array<T> of(Type<T> type, Iterable<T> data) {
        if (data instanceof Collection) {
            return of(type, (Collection<T>) data);
        }
        return builder(type, 16).add(data).build();
    }

    static <T> Array<T> of(Type<T> type, Collection<T> data) {
        return builder(type, data.size()).add(data).build();
    }

    Type<T> type();

    int size();

    T get(int index);

    Stream<T> stream();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(PrimitiveArray<?> primitive);

        void visit(GenericArray<?> generic);
    }
}
