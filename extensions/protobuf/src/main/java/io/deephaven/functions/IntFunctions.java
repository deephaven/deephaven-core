/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import java.util.Objects;
import java.util.function.Function;

public class IntFunctions {
    static <T> IntFunction<T> primitive() {
        // noinspection unchecked
        return (IntFunction<T>) IntFunctions.PrimitiveInt.INSTANCE;
    }

    static <T, R> IntFunction<T> map(Function<T, R> f, IntFunction<R> g) {
        return new IntFunctions.IntMap<>(f, g);
    }

    private enum PrimitiveInt implements IntFunction<Object> {
        INSTANCE;

        @Override
        public int applyAsInt(Object value) {
            return (int) value;
        }
    }

    private static class IntMap<T, R> implements IntFunction<T> {
        private final Function<T, R> f;
        private final IntFunction<R> g;

        public IntMap(Function<T, R> f, IntFunction<R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public int applyAsInt(T value) {
            return g.applyAsInt(f.apply(value));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            IntMap<?, ?> intMap = (IntMap<?, ?>) o;

            if (!f.equals(intMap.f))
                return false;
            return g.equals(intMap.g);
        }

        @Override
        public int hashCode() {
            int result = f.hashCode();
            result = 31 * result + g.hashCode();
            return result;
        }
    }
}
