//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import java.util.Objects;
import java.util.function.Function;

class IntFunctions {
    static <T> ToIntFunction<T> cast() {
        return cast(PrimitiveInt.INSTANCE);
    }

    static <T> ToIntFunction<T> cast(ToIntFunction<? super T> f) {
        // noinspection unchecked
        return (ToIntFunction<T>) f;
    }

    static <T> ToIntFunction<T> of(java.util.function.ToIntFunction<? super T> f) {
        return f instanceof ToIntFunction
                ? cast((ToIntFunction<? super T>) f)
                : f::applyAsInt;
    }

    static <T, R> ToIntFunction<T> map(
            Function<? super T, ? extends R> f,
            java.util.function.ToIntFunction<? super R> g) {
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
        private final Function<? super T, ? extends R> f;
        private final java.util.function.ToIntFunction<? super R> g;

        public IntMap(Function<? super T, ? extends R> f, java.util.function.ToIntFunction<? super R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public int applyAsInt(T value) {
            return g.applyAsInt(f.apply(value));
        }
    }
}
