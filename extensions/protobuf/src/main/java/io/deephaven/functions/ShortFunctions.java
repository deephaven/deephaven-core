/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import java.util.Objects;
import java.util.function.Function;

class ShortFunctions {

    static <T> ToShortFunction<T> cast() {
        // noinspection unchecked
        return (ToShortFunction<T>) PrimitiveShort.INSTANCE;
    }

    static <T, R> ToShortFunction<T> map(Function<T, R> f, ToShortFunction<R> g) {
        return new ShortMap<>(f, g);
    }

    private enum PrimitiveShort implements ToShortFunction<Object> {
        INSTANCE;

        @Override
        public short applyAsShort(Object value) {
            return (short) value;
        }
    }

    private static class ShortMap<T, R> implements ToShortFunction<T> {
        private final Function<T, R> f;
        private final ToShortFunction<R> g;

        public ShortMap(Function<T, R> f, ToShortFunction<R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public short applyAsShort(T value) {
            return g.applyAsShort(f.apply(value));
        }
    }
}
