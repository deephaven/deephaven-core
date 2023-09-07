/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import java.util.Objects;
import java.util.function.Function;

class IntFunctions {
    static <T> ToIntFunction<T> cast() {
        // noinspection unchecked
        return (ToIntFunction<T>) PrimitiveInt.INSTANCE;
    }

    static <T> ToIntFunction<T> of(java.util.function.ToIntFunction<T> f) {
        return f instanceof ToIntFunction ? (ToIntFunction<T>) f : f::applyAsInt;
    }

    static <T, R> ToIntFunction<T> map(Function<T, R> f, java.util.function.ToIntFunction<R> g) {
        return new IntMap<>(f, g);
    }

    private enum PrimitiveInt implements ToIntFunction<Object> {
        INSTANCE;

        @Override
        public int applyAsInt(Object value) {
            return (int) value;
        }
    }

    private static class IntMap<T, R> implements ToIntFunction<T> {
        private final Function<T, R> f;
        private final java.util.function.ToIntFunction<R> g;

        public IntMap(Function<T, R> f, java.util.function.ToIntFunction<R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public int applyAsInt(T value) {
            return g.applyAsInt(f.apply(value));
        }
    }
}
