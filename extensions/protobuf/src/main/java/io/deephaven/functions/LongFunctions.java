/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import java.util.Objects;
import java.util.function.Function;

class LongFunctions {

    static <T> ToLongFunction<T> cast() {
        // noinspection unchecked
        return (ToLongFunction<T>) PrimitiveLong.INSTANCE;
    }

    static <T> ToLongFunction<T> of(java.util.function.ToLongFunction<T> f) {
        return f instanceof ToLongFunction ? (ToLongFunction<T>) f : f::applyAsLong;
    }

    static <T, R> ToLongFunction<T> map(Function<T, R> f, java.util.function.ToLongFunction<R> g) {
        return new LongMap<>(f, g);
    }

    private enum PrimitiveLong implements ToLongFunction<Object> {
        INSTANCE;

        @Override
        public long applyAsLong(Object value) {
            return (long) value;
        }
    }

    private static class LongMap<T, R> implements ToLongFunction<T> {
        private final Function<T, R> f;
        private final java.util.function.ToLongFunction<R> g;

        public LongMap(Function<T, R> f, java.util.function.ToLongFunction<R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public long applyAsLong(T value) {
            return g.applyAsLong(f.apply(value));
        }
    }
}
