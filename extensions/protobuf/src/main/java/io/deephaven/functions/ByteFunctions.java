/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import java.util.Objects;
import java.util.function.Function;

class ByteFunctions {
    static <T> ToByteFunction<T> cast() {
        return cast(PrimitiveByte.INSTANCE);
    }

    static <T> ToByteFunction<T> cast(ToByteFunction<? super T> f) {
        // noinspection unchecked
        return (ToByteFunction<T>) f;
    }

    static <T, R> ToByteFunction<T> map(
            Function<? super T, ? extends R> f,
            ToByteFunction<? super R> g) {
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
        private final Function<? super T, ? extends R> f;
        private final ToByteFunction<? super R> g;

        public ByteMap(Function<? super T, ? extends R> f, ToByteFunction<? super R> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public byte applyAsByte(T value) {
            return g.applyAsByte(f.apply(value));
        }
    }
}
