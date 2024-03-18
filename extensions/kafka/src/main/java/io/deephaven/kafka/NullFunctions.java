//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.Type;
import io.deephaven.function.ToBooleanFunction;
import io.deephaven.function.ToByteFunction;
import io.deephaven.function.ToCharFunction;
import io.deephaven.function.ToDoubleFunction;
import io.deephaven.function.ToFloatFunction;
import io.deephaven.function.ToIntFunction;
import io.deephaven.function.ToLongFunction;
import io.deephaven.function.ToObjectFunction;
import io.deephaven.function.ToPrimitiveFunction;
import io.deephaven.function.ToShortFunction;
import io.deephaven.function.TypedFunction;
import io.deephaven.util.QueryConstants;

import java.util.Optional;

class NullFunctions {

    public static <T> Optional<TypedFunction<T>> of(Type<?> returnType) {
        // noinspection unchecked
        return Optional.ofNullable((TypedFunction<T>) returnType.walk(NullFunctionVisitor.INSTANCE));
    }

    public static <T> Optional<ToPrimitiveFunction<T>> of(PrimitiveType<?> returnType) {
        // noinspection unchecked
        return Optional.ofNullable((ToPrimitiveFunction<T>) returnType
                .walk((PrimitiveType.Visitor<ToPrimitiveFunction<?>>) NullFunctionVisitor.INSTANCE));
    }

    public static <T, R> ToObjectFunction<T, R> of(GenericType<R> returnType) {
        return ToObjectFunction.of(x -> null, returnType);
    }

    public static <T> ToCharFunction<T> nullCharFunction() {
        return x -> QueryConstants.NULL_CHAR;
    }

    public static <T> ToByteFunction<T> nullByteFunction() {
        return x -> QueryConstants.NULL_BYTE;
    }

    public static <T> ToShortFunction<T> nullShortFunction() {
        return x -> QueryConstants.NULL_SHORT;
    }

    public static <T> ToIntFunction<T> nullIntFunction() {
        return x -> QueryConstants.NULL_INT;
    }

    public static <T> ToLongFunction<T> nullLongFunction() {
        return x -> QueryConstants.NULL_LONG;
    }

    public static <T> ToFloatFunction<T> nullFloatFunction() {
        return x -> QueryConstants.NULL_FLOAT;
    }

    public static <T> ToDoubleFunction<T> nullDoubleFunction() {
        return x -> QueryConstants.NULL_DOUBLE;
    }

    private enum NullFunctionVisitor
            implements Type.Visitor<TypedFunction<?>>, PrimitiveType.Visitor<ToPrimitiveFunction<?>> {
        INSTANCE;

        @Override
        public ToPrimitiveFunction<?> visit(PrimitiveType<?> primitiveType) {
            return of(primitiveType).orElse(null);
        }

        @Override
        public TypedFunction<?> visit(GenericType<?> genericType) {
            return of(genericType);
        }

        @Override
        public ToBooleanFunction<?> visit(BooleanType booleanType) {
            return null;
        }

        @Override
        public ToByteFunction<?> visit(ByteType byteType) {
            return nullByteFunction();
        }

        @Override
        public ToPrimitiveFunction<?> visit(CharType charType) {
            return nullCharFunction();
        }

        @Override
        public ToPrimitiveFunction<?> visit(ShortType shortType) {
            return nullShortFunction();
        }

        @Override
        public ToPrimitiveFunction<?> visit(IntType intType) {
            return nullIntFunction();
        }

        @Override
        public ToPrimitiveFunction<?> visit(LongType longType) {
            return nullLongFunction();
        }

        @Override
        public ToPrimitiveFunction<?> visit(FloatType floatType) {
            return nullFloatFunction();
        }

        @Override
        public ToPrimitiveFunction<?> visit(DoubleType doubleType) {
            return nullDoubleFunction();
        }
    }
}
