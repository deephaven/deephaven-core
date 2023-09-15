/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

class ObjectFunctions {

    static <T> ToObjectFunction<T, Object> identity() {
        return cast(Identity.INSTANCE);
    }

    static <T, R> ToObjectFunction<T, R> cast(ToObjectFunction<? super T, ? extends R> f) {
        // noinspection unchecked
        return (ToObjectFunction<T, R>) f;
    }

    static <T, R> ToObjectFunction<T, R> cast(GenericType<R> type) {
        return new Casted<>(type);
    }

    static <T, R> ToObjectFunction<T, R> of(
            Function<? super T, ? extends R> f,
            GenericType<R> returnType) {
        return f instanceof ToObjectFunction
                ? castOrMapCast((ToObjectFunction<? super T, ? extends R>) f, returnType)
                : new FunctionImpl<>(f, returnType);
    }

    static <T, R> ToObjectFunction<T, R> castOrMapCast(
            ToObjectFunction<? super T, ?> f,
            GenericType<R> returnType) {
        // noinspection unchecked
        return f.returnType().equals(returnType)
                ? (ToObjectFunction<T, R>) f
                : f.mapToObj(ObjectFunctions.cast(returnType));
    }

    static <T, R, Z> ToObjectFunction<T, Z> map(
            Function<? super T, ? extends R> f,
            ToObjectFunction<? super R, Z> g) {
        return new ObjectMap<>(f, g, g.returnType());
    }

    static <T, R, Z> ToObjectFunction<T, Z> map(
            Function<? super T, ? extends R> f,
            Function<? super R, ? extends Z> g,
            GenericType<Z> returnType) {
        return new ObjectMap<>(f, g, returnType);
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
        public <T2> ToBooleanFunction<T2> mapToBoolean(Predicate<? super Object> g) {
            return BooleanFunctions.of(g);
        }


        @Override
        public <T2> ToCharFunction<T2> mapToChar(ToCharFunction<? super Object> g) {
            return CharFunctions.cast(g);
        }

        @Override
        public <T2> ToByteFunction<T2> mapToByte(ToByteFunction<? super Object> g) {
            return ByteFunctions.cast(g);
        }

        @Override
        public <T2> ToShortFunction<T2> mapToShort(ToShortFunction<? super Object> g) {
            return ShortFunctions.cast(g);
        }

        @Override
        public <T2> ToIntFunction<T2> mapToInt(java.util.function.ToIntFunction<? super Object> g) {
            return IntFunctions.of(g);
        }

        @Override
        public <T2> ToLongFunction<T2> mapToLong(java.util.function.ToLongFunction<? super Object> g) {
            return LongFunctions.of(g);
        }

        @Override
        public <T2> ToFloatFunction<T2> mapToFloat(ToFloatFunction<? super Object> g) {
            return FloatFunctions.cast(g);
        }

        @Override
        public <T2> ToDoubleFunction<T2> mapToDouble(java.util.function.ToDoubleFunction<? super Object> g) {
            return DoubleFunctions.of(g);
        }

        @Override
        public <T2, R2> ToObjectFunction<T2, R2> mapToObj(ToObjectFunction<? super Object, R2> g) {
            return ObjectFunctions.cast(g);
        }

        @Override
        public <T2, R2> ToObjectFunction<T2, R2> mapToObj(Function<? super Object, ? extends R2> g,
                GenericType<R2> returnType) {
            return ObjectFunctions.of(g, returnType);
        }

        @Override
        public <T2> ToPrimitiveFunction<T2> mapToPrimitive(ToPrimitiveFunction<? super Object> g) {
            return PrimitiveFunctions.cast(g);
        }

        @Override
        public <T2> TypedFunction<T2> map(TypedFunction<? super Object> g) {
            return TypedFunctions.cast(g);
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
    }

    private static final class FunctionImpl<T, R> implements ToObjectFunction<T, R> {
        private final Function<? super T, ? extends R> f;
        private final GenericType<R> returnType;

        FunctionImpl(Function<? super T, ? extends R> f, GenericType<R> returnType) {
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
    }

    private static class ObjectMap<T, R, Z> implements ToObjectFunction<T, Z> {
        private final Function<? super T, ? extends R> f;
        private final Function<? super R, ? extends Z> g;
        private final GenericType<Z> returnType;

        public ObjectMap(Function<? super T, ? extends R> f, Function<? super R, ? extends Z> g,
                GenericType<Z> returnType) {
            this.f = Objects.requireNonNull(f);
            this.g = Objects.requireNonNull(g);
            this.returnType = Objects.requireNonNull(returnType);
        }

        @Override
        public GenericType<Z> returnType() {
            return returnType;
        }

        @Override
        public Z apply(T value) {
            return g.apply(f.apply(value));
        }
    }

}
