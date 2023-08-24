/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import io.deephaven.functions.ByteFunction;
import io.deephaven.functions.LongFunction;
import io.deephaven.functions.ObjectFunction;
import io.deephaven.functions.PrimitiveFunction;
import io.deephaven.functions.TypedFunction;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.StringType;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;

import java.time.Instant;
import java.util.Objects;

import static io.deephaven.kafka.UnboxTransform.unboxByte;
import static io.deephaven.kafka.UnboxTransform.unboxChar;
import static io.deephaven.kafka.UnboxTransform.unboxDouble;
import static io.deephaven.kafka.UnboxTransform.unboxFloat;
import static io.deephaven.kafka.UnboxTransform.unboxInt;
import static io.deephaven.kafka.UnboxTransform.unboxLong;
import static io.deephaven.kafka.UnboxTransform.unboxShort;

class CommonTransform {

    private static final ByteFunction<Boolean> BOOLEAN_AS_BYTE = BooleanUtils::booleanAsByte;
    private static final LongFunction<Instant> EPOCH_NANOS = DateTimeUtils::epochNanos;

    /**
     * Potentially transform the {@link TypedFunction function} {@code f} into a common... todo
     *
     * @param f the function
     * @return the potentially transformed function
     * @param <T> the input type
     * @see #of(PrimitiveFunction)
     * @see #of(ObjectFunction)
     */
    public static <T> TypedFunction<T> of(TypedFunction<T> f) {
        return FunctionVisitor.of(f);
    }

    public static <T> TypedFunction<T> of(PrimitiveFunction<T> f) {
        return f;
    }

    /**
     * Potentially transforms the {@link ObjectFunction Object function} {@code f} into a common... todo
     *
     * @param f the Object function
     * @return the potentially transformed function
     * @param <T> the input type
     * @see #toEpochNanos(ObjectFunction)
     * @see #unboxBooleanAsByte(ObjectFunction)
     * @see UnboxTransform#unboxByte(ObjectFunction)
     * @see UnboxTransform#unboxChar(ObjectFunction)
     * @see UnboxTransform#unboxDouble(ObjectFunction)
     * @see UnboxTransform#unboxFloat(ObjectFunction)
     * @see UnboxTransform#unboxInt(ObjectFunction)
     * @see UnboxTransform#unboxLong(ObjectFunction)
     * @see UnboxTransform#unboxShort(ObjectFunction)
     */
    public static <T> TypedFunction<T> of(ObjectFunction<T, ?> f) {
        return ObjectFunctionVisitor.of(f);
    }

    /**
     * Equivalent to {@code f.mapByte(BooleanUtils::booleanAsByte)}.
     *
     * @param f the Boolean function
     * @return the byte function
     * @param <T> the input type
     * @see BooleanUtils#booleanAsByte(Boolean)
     */
    public static <T> ByteFunction<T> unboxBooleanAsByte(ObjectFunction<T, Boolean> f) {
        return f.mapByte(BOOLEAN_AS_BYTE);
    }

    /**
     * Equivalent to {@code f.mapLong(DateTimeUtils::epochNanos)}.
     *
     * @param f the instant function
     * @return the epoch nanos function
     * @param <T> the input type
     * @see DateTimeUtils#epochNanos(Instant)
     */
    public static <T> LongFunction<T> toEpochNanos(ObjectFunction<T, Instant> f) {
        return f.mapLong(EPOCH_NANOS);
    }

    private static class FunctionVisitor<T> implements TypedFunction.Visitor<T, TypedFunction<T>> {

        public static <T> TypedFunction<T> of(TypedFunction<T> f) {
            return f.walk(new FunctionVisitor<>());
        }

        private FunctionVisitor() {}

        @Override
        public TypedFunction<T> visit(PrimitiveFunction<T> f) {
            return CommonTransform.of(f);
        }

        @Override
        public TypedFunction<T> visit(ObjectFunction<T, ?> f) {
            return CommonTransform.of(f);
        }
    }

    private static class ObjectFunctionVisitor<T> implements
            GenericType.Visitor<TypedFunction<T>>,
            BoxedType.Visitor<TypedFunction<T>> {

        public static <T> TypedFunction<T> of(ObjectFunction<T, ?> f) {
            return f.returnType().walk(new ObjectFunctionVisitor<>(f));
        }

        private final ObjectFunction<T, ?> f;

        private ObjectFunctionVisitor(ObjectFunction<T, ?> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public TypedFunction<T> visit(BoxedType<?> boxedType) {
            return boxedType.walk((BoxedType.Visitor<TypedFunction<T>>) this);
        }

        @Override
        public TypedFunction<T> visit(InstantType instantType) {
            return toEpochNanos(f.cast(instantType));
        }

        @Override
        public TypedFunction<T> visit(StringType stringType) {
            return f;
        }

        @Override
        public TypedFunction<T> visit(ArrayType<?, ?> arrayType) {
            return f;
        }

        @Override
        public TypedFunction<T> visit(CustomType<?> customType) {
            return f;
        }

        @Override
        public TypedFunction<T> visit(BoxedBooleanType booleanType) {
            return unboxBooleanAsByte(f.cast(booleanType));
        }

        @Override
        public TypedFunction<T> visit(BoxedByteType byteType) {
            return unboxByte(f.cast(byteType));
        }

        @Override
        public TypedFunction<T> visit(BoxedCharType charType) {
            return unboxChar(f.cast(charType));
        }

        @Override
        public TypedFunction<T> visit(BoxedShortType shortType) {
            return unboxShort(f.cast(shortType));
        }

        @Override
        public TypedFunction<T> visit(BoxedIntType intType) {
            return unboxInt(f.cast(intType));
        }

        @Override
        public TypedFunction<T> visit(BoxedLongType longType) {
            return unboxLong(f.cast(longType));
        }

        @Override
        public TypedFunction<T> visit(BoxedFloatType floatType) {
            return unboxFloat(f.cast(floatType));
        }

        @Override
        public TypedFunction<T> visit(BoxedDoubleType doubleType) {
            return unboxDouble(f.cast(doubleType));
        }
    }
}
