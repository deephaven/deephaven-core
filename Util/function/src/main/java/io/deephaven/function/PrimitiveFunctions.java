//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import java.util.Objects;

class PrimitiveFunctions {
    static <T> ToPrimitiveFunction<T> cast(ToPrimitiveFunction<? super T> f) {
        // noinspection unchecked
        return (ToPrimitiveFunction<T>) f;
    }

    static <T, R> ToPrimitiveFunction<T> map(
            ToObjectFunction<? super T, ? extends R> f,
            ToPrimitiveFunction<? super R> g) {
        return MapPrimitiveVisitor.of(f, g);
    }

    private static class MapPrimitiveVisitor<T, R>
            implements ToPrimitiveFunction.Visitor<T, ToPrimitiveFunction<R>> {

        public static <T, R> ToPrimitiveFunction<R> of(
                ToObjectFunction<? super R, ? extends T> f,
                ToPrimitiveFunction<? super T> g) {
            return g.walk(new MapPrimitiveVisitor<>(f));
        }

        private final ToObjectFunction<? super R, ? extends T> f;

        private MapPrimitiveVisitor(ToObjectFunction<? super R, ? extends T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public ToBooleanFunction<R> visit(ToBooleanFunction<T> g) {
            return f.mapToBoolean(g);
        }

        @Override
        public ToCharFunction<R> visit(ToCharFunction<T> g) {
            return f.mapToChar(g);
        }

        @Override
        public ToByteFunction<R> visit(ToByteFunction<T> g) {
            return f.mapToByte(g);
        }

        @Override
        public ToShortFunction<R> visit(ToShortFunction<T> g) {
            return f.mapToShort(g);
        }

        @Override
        public ToIntFunction<R> visit(ToIntFunction<T> g) {
            return f.mapToInt(g);
        }

        @Override
        public ToLongFunction<R> visit(ToLongFunction<T> g) {
            return f.mapToLong(g);
        }

        @Override
        public ToFloatFunction<R> visit(ToFloatFunction<T> g) {
            return f.mapToFloat(g);
        }

        @Override
        public ToDoubleFunction<R> visit(ToDoubleFunction<T> g) {
            return f.mapToDouble(g);
        }
    }
}
