/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
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
import io.deephaven.functions.BooleanFunction;
import io.deephaven.functions.ByteFunction;
import io.deephaven.functions.CharFunction;
import io.deephaven.functions.DoubleFunction;
import io.deephaven.functions.FloatFunction;
import io.deephaven.functions.IntFunction;
import io.deephaven.functions.LongFunction;
import io.deephaven.functions.ObjectFunction;
import io.deephaven.functions.PrimitiveFunction;
import io.deephaven.functions.ShortFunction;
import io.deephaven.functions.TypedFunction;
import io.deephaven.util.QueryConstants;

import java.util.Optional;

class NullFunctions {

    public static <T> Optional<TypedFunction<T>> of(Type<?> returnType) {
        // noinspection unchecked
        return Optional.ofNullable((TypedFunction<T>) returnType.walk(NullFunctionVisitor.INSTANCE));
    }

    public static <T> Optional<PrimitiveFunction<T>> of(PrimitiveType<?> returnType) {
        // noinspection unchecked
        return Optional.ofNullable((PrimitiveFunction<T>) returnType
                .walk((PrimitiveType.Visitor<PrimitiveFunction<?>>) NullFunctionVisitor.INSTANCE));
    }

    public static <T, R> ObjectFunction<T, R> of(GenericType<R> returnType) {
        return ObjectFunction.of(x -> null, returnType);
    }

    public static <T> CharFunction<T> nullCharFunction() {
        return x -> QueryConstants.NULL_CHAR;
    }

    public static <T> ByteFunction<T> nullByteFunction() {
        return x -> QueryConstants.NULL_BYTE;
    }

    public static <T> ShortFunction<T> nullShortFunction() {
        return x -> QueryConstants.NULL_SHORT;
    }

    public static <T> IntFunction<T> nullIntFunction() {
        return x -> QueryConstants.NULL_INT;
    }

    public static <T> LongFunction<T> nullLongFunction() {
        return x -> QueryConstants.NULL_LONG;
    }

    public static <T> FloatFunction<T> nullFloatFunction() {
        return x -> QueryConstants.NULL_FLOAT;
    }

    public static <T> DoubleFunction<T> nullDoubleFunction() {
        return x -> QueryConstants.NULL_DOUBLE;
    }

    private enum NullFunctionVisitor
            implements Type.Visitor<TypedFunction<?>>, PrimitiveType.Visitor<PrimitiveFunction<?>> {
        INSTANCE;

        @Override
        public PrimitiveFunction<?> visit(PrimitiveType<?> primitiveType) {
            return of(primitiveType).orElse(null);
        }

        @Override
        public TypedFunction<?> visit(GenericType<?> genericType) {
            return of(genericType);
        }

        @Override
        public BooleanFunction<?> visit(BooleanType booleanType) {
            return null;
        }

        @Override
        public ByteFunction<?> visit(ByteType byteType) {
            return nullByteFunction();
        }

        @Override
        public PrimitiveFunction<?> visit(CharType charType) {
            return nullCharFunction();
        }

        @Override
        public PrimitiveFunction<?> visit(ShortType shortType) {
            return nullShortFunction();
        }

        @Override
        public PrimitiveFunction<?> visit(IntType intType) {
            return nullIntFunction();
        }

        @Override
        public PrimitiveFunction<?> visit(LongType longType) {
            return nullLongFunction();
        }

        @Override
        public PrimitiveFunction<?> visit(FloatType floatType) {
            return nullFloatFunction();
        }

        @Override
        public PrimitiveFunction<?> visit(DoubleType doubleType) {
            return nullDoubleFunction();
        }
    }
}
