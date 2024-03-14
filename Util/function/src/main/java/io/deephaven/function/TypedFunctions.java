//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import java.util.Objects;

class TypedFunctions {
    static <T> TypedFunction<T> cast(TypedFunction<? super T> f) {
        // noinspection unchecked
        return (TypedFunction<T>) f;
    }

    static <T, R> TypedFunction<T> map(
            ToObjectFunction<? super T, ? extends R> f,
            TypedFunction<? super R> g) {
        return MapVisitor.of(f, g);
    }

    private static class MapVisitor<T, R> implements TypedFunction.Visitor<T, TypedFunction<R>> {

        public static <T, R> TypedFunction<R> of(ToObjectFunction<? super R, ? extends T> f,
                TypedFunction<? super T> g) {
            return g.walk(new MapVisitor<>(f));
        }

        private final ToObjectFunction<? super R, ? extends T> f;

        private MapVisitor(ToObjectFunction<? super R, ? extends T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public ToPrimitiveFunction<R> visit(ToPrimitiveFunction<T> g) {
            return f.mapToPrimitive(g);
        }

        @Override
        public ToObjectFunction<R, ?> visit(ToObjectFunction<T, ?> g) {
            return f.mapToObj(g);
        }
    }
}
