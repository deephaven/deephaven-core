/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import java.util.Objects;
import java.util.function.Function;

class LongFunctions {

    static <T> LongFunction<T> primitive() {
        // noinspection unchecked
        return (LongFunction<T>) PrimitiveLong.INSTANCE;
    }

    static <T, R> LongFunction<T> map(Function<T, R> f, LongFunction<R> g) {
        return new LongMap<>(f, g);
    }

    private enum PrimitiveLong implements LongFunction<Object> {
        INSTANCE;

        @Override
        public long applyAsLong(Object value) {
            return (long) value;
        }
    }

    private static class LongMap<T, R> implements LongFunction<T> {
        private final Function<T, R> f;
        private final LongFunction<R> g;

        public LongMap(Function<T, R> f, LongFunction<R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public long applyAsLong(T value) {
            return g.applyAsLong(f.apply(value));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            LongMap<?, ?> longMap = (LongMap<?, ?>) o;

            if (!f.equals(longMap.f))
                return false;
            return g.equals(longMap.g);
        }

        @Override
        public int hashCode() {
            int result = f.hashCode();
            result = 31 * result + g.hashCode();
            return result;
        }
    }
}
