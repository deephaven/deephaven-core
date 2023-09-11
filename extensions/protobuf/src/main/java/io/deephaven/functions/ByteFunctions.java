/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import java.util.Objects;
import java.util.function.Function;

class ByteFunctions {
    static <T> ToByteFunction<T> cast() {
        // noinspection unchecked
        return (ToByteFunction<T>) PrimitiveByte.INSTANCE;
    }

    static <T, R> ToByteFunction<T> map(Function<T, R> f, ToByteFunction<R> g) {
        return new ByteMap<>(f, g);
    }

    private enum PrimitiveByte implements ToByteFunction<Object> {
        INSTANCE;

        @Override
        public byte applyAsByte(Object value) {
            return (byte) value;
        }
    }

    private static class ByteMap<T, R> implements ToByteFunction<T> {
        private final Function<T, R> f;
        private final ToByteFunction<R> g;

        public ByteMap(Function<T, R> f, ToByteFunction<R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public byte applyAsByte(T value) {
            return g.applyAsByte(f.apply(value));
        }
    }
}
