//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import java.util.Objects;
import java.util.function.Function;

class LongFunctions {

    static <T> ToLongFunction<T> cast() {
        return cast(PrimitiveLong.INSTANCE);
    }

    static <T> ToLongFunction<T> cast(ToLongFunction<? super T> f) {
        // noinspection unchecked
        return (ToLongFunction<T>) f;
    }

    static <T> ToLongFunction<T> of(java.util.function.ToLongFunction<? super T> f) {
        return f instanceof ToLongFunction
                ? cast((ToLongFunction<? super T>) f)
                : f::applyAsLong;
    }

    static <T, R> ToLongFunction<T> map(
            Function<? super T, ? extends R> f,
            java.util.function.ToLongFunction<? super R> g) {
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
        private final Function<? super T, ? extends R> f;
        private final java.util.function.ToLongFunction<? super R> g;

        public LongMap(Function<? super T, ? extends R> f, java.util.function.ToLongFunction<? super R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public long applyAsLong(T value) {
            return g.applyAsLong(f.apply(value));
        }
    }
}
