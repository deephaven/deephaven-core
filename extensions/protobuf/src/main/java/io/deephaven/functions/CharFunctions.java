/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import java.util.Objects;
import java.util.function.Function;

class CharFunctions {
    static <T> ToCharFunction<T> primitive() {
        // noinspection unchecked
        return (ToCharFunction<T>) PrimitiveChar.INSTANCE;
    }

    static <T, R> ToCharFunction<T> map(Function<T, R> f, ToCharFunction<R> g) {
        return new CharMap<>(f, g);
    }

    private enum PrimitiveChar implements ToCharFunction<Object> {
        INSTANCE;

        @Override
        public char applyAsChar(Object value) {
            return (char) value;
        }
    }

    private static class CharMap<T, R> implements ToCharFunction<T> {
        private final Function<T, R> f;
        private final ToCharFunction<R> g;

        public CharMap(Function<T, R> f, ToCharFunction<R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public char applyAsChar(T value) {
            return g.applyAsChar(f.apply(value));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            CharMap<?, ?> charMap = (CharMap<?, ?>) o;

            if (!f.equals(charMap.f))
                return false;
            return g.equals(charMap.g);
        }

        @Override
        public int hashCode() {
            int result = f.hashCode();
            result = 31 * result + g.hashCode();
            return result;
        }
    }
}
