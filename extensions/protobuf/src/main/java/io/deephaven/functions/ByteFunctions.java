/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import java.util.Objects;
import java.util.function.Function;

class ByteFunctions {
    static <T> ByteFunction<T> primitive() {
        // noinspection unchecked
        return (ByteFunction<T>) PrimitiveByte.INSTANCE;
    }

    static <T, R> ByteFunction<T> map(Function<T, R> f, ByteFunction<R> g) {
        return new ByteMap<>(f, g);
    }

    private enum PrimitiveByte implements ByteFunction<Object> {
        INSTANCE;

        @Override
        public byte applyAsByte(Object value) {
            return (byte) value;
        }
    }

    private static class ByteMap<T, R> implements ByteFunction<T> {
        private final Function<T, R> f;
        private final ByteFunction<R> g;

        public ByteMap(Function<T, R> f, ByteFunction<R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public byte applyAsByte(T value) {
            return g.applyAsByte(f.apply(value));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            ByteMap<?, ?> byteMap = (ByteMap<?, ?>) o;

            if (!f.equals(byteMap.f))
                return false;
            return g.equals(byteMap.g);
        }

        @Override
        public int hashCode() {
            int result = f.hashCode();
            result = 31 * result + g.hashCode();
            return result;
        }
    }
}
