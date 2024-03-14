//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import java.util.Objects;
import java.util.function.Function;

class ShortFunctions {

    static <T> ToShortFunction<T> cast() {
        return cast(PrimitiveShort.INSTANCE);
    }

    static <T> ToShortFunction<T> cast(ToShortFunction<? super T> f) {
        // noinspection unchecked
        return (ToShortFunction<T>) f;
    }

    static <T, R> ToShortFunction<T> map(
            Function<? super T, ? extends R> f,
            ToShortFunction<? super R> g) {
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
        private final Function<? super T, ? extends R> f;
        private final ToShortFunction<? super R> g;

        public ShortMap(Function<? super T, ? extends R> f, ToShortFunction<? super R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public short applyAsShort(T value) {
            return g.applyAsShort(f.apply(value));
        }
    }
}
