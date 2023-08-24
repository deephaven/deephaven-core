/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import java.util.Objects;
import java.util.function.Function;

public class ShortFunctions {

    static <T> ShortFunction<T> primitive() {
        // noinspection unchecked
        return (ShortFunction<T>) PrimitiveShort.INSTANCE;
    }

    static <T, R> ShortFunction<T> map(Function<T, R> f, ShortFunction<R> g) {
        return new ShortMap<>(f, g);
    }

    private enum PrimitiveShort implements ShortFunction<Object> {
        INSTANCE;

        @Override
        public short applyAsShort(Object value) {
            return (short) value;
        }
    }

    private static class ShortMap<T, R> implements ShortFunction<T> {
        private final Function<T, R> f;
        private final ShortFunction<R> g;

        public ShortMap(Function<T, R> f, ShortFunction<R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public short applyAsShort(T value) {
            return g.applyAsShort(f.apply(value));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            ShortMap<?, ?> shortMap = (ShortMap<?, ?>) o;

            if (!f.equals(shortMap.f))
                return false;
            return g.equals(shortMap.g);
        }

        @Override
        public int hashCode() {
            int result = f.hashCode();
            result = 31 * result + g.hashCode();
            return result;
        }
    }
}
