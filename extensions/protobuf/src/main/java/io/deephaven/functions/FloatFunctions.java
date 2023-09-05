/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import java.util.Objects;
import java.util.function.Function;

class FloatFunctions {
    static <T> ToFloatFunction<T> primitive() {
        // noinspection unchecked
        return (ToFloatFunction<T>) PrimitiveFloat.INSTANCE;
    }

    static <T, R> ToFloatFunction<T> map(Function<T, R> f, ToFloatFunction<R> g) {
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
        private final Function<T, R> f;
        private final ToFloatFunction<R> g;

        public FloatMap(Function<T, R> f, ToFloatFunction<R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public float applyAsFloat(T value) {
            return g.applyAsFloat(f.apply(value));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            FloatMap<?, ?> floatMap = (FloatMap<?, ?>) o;

            if (!f.equals(floatMap.f))
                return false;
            return g.equals(floatMap.g);
        }

        @Override
        public int hashCode() {
            int result = f.hashCode();
            result = 31 * result + g.hashCode();
            return result;
        }
    }
}
