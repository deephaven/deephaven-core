/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import java.util.Objects;
import java.util.function.Function;

class FloatFunctions {
    static <T> FloatFunction<T> primitive() {
        // noinspection unchecked
        return (FloatFunction<T>) PrimitiveFloat.INSTANCE;
    }

    static <T, R> FloatFunction<T> map(Function<T, R> f, FloatFunction<R> g) {
        return new FloatMap<>(f, g);
    }

    private enum PrimitiveFloat implements FloatFunction<Object> {
        INSTANCE;

        @Override
        public float applyAsFloat(Object value) {
            return (float) value;
        }
    }

    private static class FloatMap<T, R> implements FloatFunction<T> {
        private final Function<T, R> f;
        private final FloatFunction<R> g;

        public FloatMap(Function<T, R> f, FloatFunction<R> g) {
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
