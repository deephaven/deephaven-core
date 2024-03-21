//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.array;

import io.deephaven.qst.type.Type;

import java.util.Collection;

/**
 * Provides strong typing around an array-like object. Has a definite {@link #size() size}.
 *
 * @param <T> the item type
 * @see PrimitiveArray
 * @see GenericArray
 */
public interface Array<T> {

    static <T> ArrayBuilder<T, ?, ?> builder(Type<T> type) {
        return TypeToArrayBuilder.of(type, Util.DEFAULT_BUILDER_INITIAL_CAPACITY);
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
        return builder(type, Util.DEFAULT_BUILDER_INITIAL_CAPACITY).add(data).build();
    }

    static <T> Array<T> of(Type<T> type, Collection<T> data) {
        return builder(type, data.size()).add(data).build();
    }

    Type<T> componentType();

    int size();

    <R> R walk(Visitor<R> visitor);

    interface Visitor<R> {
        R visit(PrimitiveArray<?> primitive);

        R visit(GenericArray<?> generic);
    }
}
