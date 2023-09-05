/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;

import java.util.Objects;
import java.util.function.Function;

class ObjectFunctions {

    static <T> ToObjectFunction<T, Object> identity() {
        // noinspection unchecked
        return (ToObjectFunction<T, Object>) Identity.INSTANCE;
    }

    static <T, R> ToObjectFunction<T, R> cast(GenericType<R> type) {
        return new Casted<>(type);
    }

    static <T, R> ToObjectFunction<T, R> of(Function<T, R> f, GenericType<R> returnType) {
        return new FunctionImpl<>(f, returnType);
    }

    static <T, R, Z> ToObjectFunction<T, Z> map(Function<T, R> f, ToObjectFunction<R, Z> g) {
        return new ObjectMap<>(f, g);
    }

    static <T, R> ToPrimitiveFunction<T> mapPrimitive(ToObjectFunction<T, R> f, ToPrimitiveFunction<R> g) {
        return MapPrimitiveVisitor.of(f, g);
    }

    static <T, R> TypedFunction<T> map(ToObjectFunction<T, R> f, TypedFunction<R> g) {
        return MapVisitor.of(f, g);
    }

    private enum Identity implements ToObjectFunction<Object, Object> {
        INSTANCE;

        private static final CustomType<Object> RETURN_TYPE = Type.ofCustom(Object.class);

        @Override
        public GenericType<Object> returnType() {
            return RETURN_TYPE;
        }

        @Override
        public Object apply(Object value) {
            return value;
        }

        @Override
        public ToObjectFunction<Object, Object> mapInput(Function<Object, Object> f) {
            return ToObjectFunction.of(f, RETURN_TYPE);
        }

        @Override
        public ToBooleanFunction<Object> mapBoolean(ToBooleanFunction<Object> g) {
            return g;
        }

        @Override
        public ToCharFunction<Object> mapChar(ToCharFunction<Object> g) {
            return g;
        }

        @Override
        public ToByteFunction<Object> mapByte(ToByteFunction<Object> g) {
            return g;
        }

        @Override
        public ToShortFunction<Object> mapShort(ToShortFunction<Object> g) {
            return g;
        }

        @Override
        public ToIntFunction<Object> mapInt(ToIntFunction<Object> g) {
            return g;
        }

        @Override
        public ToLongFunction<Object> mapLong(ToLongFunction<Object> g) {
            return g;
        }

        @Override
        public ToFloatFunction<Object> mapFloat(ToFloatFunction<Object> g) {
            return g;
        }

        @Override
        public ToDoubleFunction<Object> mapDouble(ToDoubleFunction<Object> g) {
            return g;
        }

        @Override
        public <R2> ToObjectFunction<Object, R2> mapObj(ToObjectFunction<Object, R2> g) {
            return g;
        }

        @Override
        public <R2> ToObjectFunction<Object, R2> mapObj(Function<Object, R2> g, GenericType<R2> returnType) {
            return ToObjectFunction.of(g, returnType);
        }

        @Override
        public ToPrimitiveFunction<Object> mapPrimitive(ToPrimitiveFunction<Object> g) {
            return g;
        }

        @Override
        public TypedFunction<Object> map(TypedFunction<Object> g) {
            return g;
        }
    }

    private static class Casted<T, R> implements ToObjectFunction<T, R> {
        private final GenericType<R> returnType;

        public Casted(GenericType<R> returnType) {
            this.returnType = Objects.requireNonNull(returnType);
        }

        @Override
        public GenericType<R> returnType() {
            return returnType;
        }

        @Override
        public R apply(T value) {
            return returnType.clazz().cast(value);
        }

        @Override
        public boolean equals(Object x) {
            if (this == x)
                return true;
            if (x == null || getClass() != x.getClass())
                return false;

            Casted<?, ?> casted = (Casted<?, ?>) x;

            return returnType.equals(casted.returnType);
        }

        @Override
        public int hashCode() {
            return returnType.hashCode();
        }
    }

    private static final class FunctionImpl<T, R> implements ToObjectFunction<T, R> {
        private final Function<T, R> f;
        private final GenericType<R> returnType;

        FunctionImpl(Function<T, R> f, GenericType<R> returnType) {
            this.f = Objects.requireNonNull(f);
            this.returnType = Objects.requireNonNull(returnType);
        }

        @Override
        public GenericType<R> returnType() {
            return returnType;
        }

        @Override
        public R apply(T value) {
            return f.apply(value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            FunctionImpl<?, ?> that = (FunctionImpl<?, ?>) o;

            if (!returnType.equals(that.returnType))
                return false;
            return f.equals(that.f);
        }

        @Override
        public int hashCode() {
            int result = returnType.hashCode();
            result = 31 * result + f.hashCode();
            return result;
        }
    }

    private static class ObjectMap<T, R, Z> implements ToObjectFunction<T, Z> {
        private final Function<T, R> f;
        private final ToObjectFunction<R, Z> g;

        public ObjectMap(Function<T, R> f, ToObjectFunction<R, Z> g) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
        }

        @Override
        public GenericType<Z> returnType() {
            return g.returnType();
        }

        @Override
        public Z apply(T value) {
            return g.apply(f.apply(value));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            ObjectMap<?, ?, ?> objectMap = (ObjectMap<?, ?, ?>) o;

            if (!f.equals(objectMap.f))
                return false;
            return g.equals(objectMap.g);
        }

        @Override
        public int hashCode() {
            int result = f.hashCode();
            result = 31 * result + g.hashCode();
            return result;
        }
    }

    private static class MapPrimitiveVisitor<T, R> implements ToPrimitiveFunction.Visitor<T, ToPrimitiveFunction<R>> {

        public static <T, R> ToPrimitiveFunction<R> of(ToObjectFunction<R, T> f, ToPrimitiveFunction<T> g) {
            return g.walk(new MapPrimitiveVisitor<>(f));
        }

        private final ToObjectFunction<R, T> f;

        private MapPrimitiveVisitor(ToObjectFunction<R, T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public ToPrimitiveFunction<R> visit(ToBooleanFunction<T> g) {
            return f.mapBoolean(g);
        }

        @Override
        public ToPrimitiveFunction<R> visit(ToCharFunction<T> g) {
            return f.mapChar(g);
        }

        @Override
        public ToPrimitiveFunction<R> visit(ToByteFunction<T> g) {
            return f.mapByte(g);
        }

        @Override
        public ToPrimitiveFunction<R> visit(ToShortFunction<T> g) {
            return f.mapShort(g);
        }

        @Override
        public ToPrimitiveFunction<R> visit(ToIntFunction<T> g) {
            return f.mapInt(g);
        }

        @Override
        public ToPrimitiveFunction<R> visit(ToLongFunction<T> g) {
            return f.mapLong(g);
        }

        @Override
        public ToPrimitiveFunction<R> visit(ToFloatFunction<T> g) {
            return f.mapFloat(g);
        }

        @Override
        public ToPrimitiveFunction<R> visit(ToDoubleFunction<T> g) {
            return f.mapDouble(g);
        }
    }

    private static class MapVisitor<T, R> implements TypedFunction.Visitor<T, TypedFunction<R>> {

        public static <T, R> TypedFunction<R> of(ToObjectFunction<R, T> f, TypedFunction<T> g) {
            return g.walk(new MapVisitor<>(f));
        }

        private final ToObjectFunction<R, T> f;

        private MapVisitor(ToObjectFunction<R, T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public TypedFunction<R> visit(ToPrimitiveFunction<T> g) {
            return f.mapPrimitive(g);
        }

        @Override
        public TypedFunction<R> visit(ToObjectFunction<T, ?> g) {
            return f.mapObj(g);
        }
    }
}
