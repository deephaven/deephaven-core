//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import java.util.Objects;
import java.util.function.Function;

class FloatFunctions {
    static <T> ToFloatFunction<T> cast() {
        return cast(PrimitiveFloat.INSTANCE);
    }

    static <T> ToFloatFunction<T> cast(ToFloatFunction<? super T> f) {
        // noinspection unchecked
        return (ToFloatFunction<T>) f;
    }

    static <T, R> ToFloatFunction<T> map(
            Function<? super T, ? extends R> f,
            ToFloatFunction<? super R> g) {
        return new FloatMap<>(f, g);
    }

    private enum PrimitiveFloat implements ToFloatFunction<Object> {
        INSTANCE;

        @Override
        public float applyAsFloat(Object value) {
            return (float) value;
        }
    }

    private static class FloatMap<T, R> implements ToFloatFunction<T> {
        private final Function<? super T, ? extends R> f;
        private final ToFloatFunction<? super R> g;

        public FloatMap(Function<? super T, ? extends R> f, ToFloatFunction<? super R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public float applyAsFloat(T value) {
            return g.applyAsFloat(f.apply(value));
        }
    }
}
