/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import java.util.Objects;
import java.util.function.Function;

class DoubleFunctions {

    static <T> ToDoubleFunction<T> primitive() {
        // noinspection unchecked
        return (ToDoubleFunction<T>) PrimitiveDouble.INSTANCE;
    }

    static <T, R> ToDoubleFunction<T> map(Function<T, R> f, ToDoubleFunction<R> g) {
        return new DoubleFunctionMap<>(f, g);
    }

    private enum PrimitiveDouble implements ToDoubleFunction<Object> {
        INSTANCE;

        @Override
        public double applyAsDouble(Object value) {
            return (double) value;
        }
    }

    private static class DoubleFunctionMap<T, R> implements ToDoubleFunction<T> {
        private final Function<T, R> f;
        private final ToDoubleFunction<R> g;

        public DoubleFunctionMap(Function<T, R> f, ToDoubleFunction<R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public double applyAsDouble(T value) {
            return g.applyAsDouble(f.apply(value));
        }
    }
}
