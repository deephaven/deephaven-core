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

    static <T> ObjectFunction<T, Object> identity() {
        // noinspection unchecked
        return (ObjectFunction<T, Object>) Identity.INSTANCE;
    }

    static <T, R> ObjectFunction<T, R> cast(GenericType<R> type) {
        return new Casted<>(type);
    }

    static <T, R> ObjectFunction<T, R> of(Function<T, R> f, GenericType<R> returnType) {
        return new FunctionImpl<>(f, returnType);
    }

    static <T, R, Z> ObjectFunction<T, Z> map(Function<T, R> f, ObjectFunction<R, Z> g) {
        return new ObjectMap<>(f, g);
    }

    static <T, R> PrimitiveFunction<T> mapPrimitive(ObjectFunction<T, R> f, PrimitiveFunction<R> g) {
        return MapPrimitiveVisitor.of(f, g);
    }

    static <T, R> TypedFunction<T> map(ObjectFunction<T, R> f, TypedFunction<R> g) {
        return MapVisitor.of(f, g);
    }

    private enum Identity implements ObjectFunction<Object, Object> {
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
        public ObjectFunction<Object, Object> mapInput(Function<Object, Object> f) {
            return ObjectFunction.of(f, RETURN_TYPE);
        }

        @Override
        public BooleanFunction<Object> mapBoolean(BooleanFunction<Object> g) {
            return g;
        }

        @Override
        public CharFunction<Object> mapChar(CharFunction<Object> g) {
            return g;
        }

        @Override
        public ByteFunction<Object> mapByte(ByteFunction<Object> g) {
            return g;
        }

        @Override
        public ShortFunction<Object> mapShort(ShortFunction<Object> g) {
            return g;
        }

        @Override
        public IntFunction<Object> mapInt(IntFunction<Object> g) {
            return g;
        }

        @Override
        public LongFunction<Object> mapLong(LongFunction<Object> g) {
            return g;
        }

        @Override
        public FloatFunction<Object> mapFloat(FloatFunction<Object> g) {
            return g;
        }

        @Override
        public DoubleFunction<Object> mapDouble(DoubleFunction<Object> g) {
            return g;
        }

        @Override
        public <R2> ObjectFunction<Object, R2> mapObj(ObjectFunction<Object, R2> g) {
            return g;
        }

        @Override
        public <R2> ObjectFunction<Object, R2> mapObj(Function<Object, R2> g, GenericType<R2> returnType) {
            return ObjectFunction.of(g, returnType);
        }

        @Override
        public PrimitiveFunction<Object> mapPrimitive(PrimitiveFunction<Object> g) {
            return g;
        }

        @Override
        public TypedFunction<Object> map(TypedFunction<Object> g) {
            return g;
        }
    }

    private static class Casted<T, R> implements ObjectFunction<T, R> {
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

    private static final class FunctionImpl<T, R> implements ObjectFunction<T, R> {
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

    private static class ObjectMap<T, R, Z> implements ObjectFunction<T, Z> {
        private final Function<T, R> f;
        private final ObjectFunction<R, Z> g;

        public ObjectMap(Function<T, R> f, ObjectFunction<R, Z> g) {
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

    private static class MapPrimitiveVisitor<T, R> implements PrimitiveFunction.Visitor<T, PrimitiveFunction<R>> {

        public static <T, R> PrimitiveFunction<R> of(ObjectFunction<R, T> f, PrimitiveFunction<T> g) {
            return g.walk(new MapPrimitiveVisitor<>(f));
        }

        private final ObjectFunction<R, T> f;

        private MapPrimitiveVisitor(ObjectFunction<R, T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public PrimitiveFunction<R> visit(BooleanFunction<T> g) {
            return f.mapBoolean(g);
        }

        @Override
        public PrimitiveFunction<R> visit(CharFunction<T> g) {
            return f.mapChar(g);
        }

        @Override
        public PrimitiveFunction<R> visit(ByteFunction<T> g) {
            return f.mapByte(g);
        }

        @Override
        public PrimitiveFunction<R> visit(ShortFunction<T> g) {
            return f.mapShort(g);
        }

        @Override
        public PrimitiveFunction<R> visit(IntFunction<T> g) {
            return f.mapInt(g);
        }

        @Override
        public PrimitiveFunction<R> visit(LongFunction<T> g) {
            return f.mapLong(g);
        }

        @Override
        public PrimitiveFunction<R> visit(FloatFunction<T> g) {
            return f.mapFloat(g);
        }

        @Override
        public PrimitiveFunction<R> visit(DoubleFunction<T> g) {
            return f.mapDouble(g);
        }
    }

    private static class MapVisitor<T, R> implements TypedFunction.Visitor<T, TypedFunction<R>> {

        public static <T, R> TypedFunction<R> of(ObjectFunction<R, T> f, TypedFunction<T> g) {
            return g.walk(new MapVisitor<>(f));
        }

        private final ObjectFunction<R, T> f;

        private MapVisitor(ObjectFunction<R, T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public TypedFunction<R> visit(PrimitiveFunction<T> g) {
            return f.mapPrimitive(g);
        }

        @Override
        public TypedFunction<R> visit(ObjectFunction<T, ?> g) {
            return f.mapObj(g);
        }
    }
}
