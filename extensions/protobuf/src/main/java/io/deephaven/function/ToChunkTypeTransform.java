//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

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

import static io.deephaven.function.UnboxTransform.unboxByte;
import static io.deephaven.function.UnboxTransform.unboxChar;
import static io.deephaven.function.UnboxTransform.unboxDouble;
import static io.deephaven.function.UnboxTransform.unboxFloat;
import static io.deephaven.function.UnboxTransform.unboxInt;
import static io.deephaven.function.UnboxTransform.unboxLong;
import static io.deephaven.function.UnboxTransform.unboxShort;

public class ToChunkTypeTransform {

    private static final ToByteFunction<Boolean> BOOLEAN_AS_BYTE = BooleanUtils::booleanAsByte;
    private static final ToLongFunction<Instant> EPOCH_NANOS = DateTimeUtils::epochNanos;

    /**
     * Transform the {@link TypedFunction function} {@code f} into its expected chunk type function.
     *
     * @param f the function
     * @return the chunk type function
     * @param <T> the input type
     * @see #of(ToPrimitiveFunction)
     * @see #of(ToObjectFunction)
     */
    public static <T> TypedFunction<T> of(TypedFunction<T> f) {
        return FunctionVisitor.of(f);
    }

    /**
     * Transform the {@link ToPrimitiveFunction function} {@code f} into its expected chunk type function.
     *
     * @param f the function
     * @return the chunk type function
     * @param <T> the input type
     */
    public static <T> TypedFunction<T> of(ToPrimitiveFunction<T> f) {
        return f;
    }

    /**
     * Transform the {@link ToPrimitiveFunction function} {@code f} into its expected chunk type function.
     *
     * @param f the Object function
     * @return the chunk type function
     * @param <T> the input type
     * @see #toEpochNanos(ToObjectFunction)
     * @see #unboxBooleanAsByte(ToObjectFunction)
     * @see UnboxTransform#unboxByte(ToObjectFunction)
     * @see UnboxTransform#unboxChar(ToObjectFunction)
     * @see UnboxTransform#unboxDouble(ToObjectFunction)
     * @see UnboxTransform#unboxFloat(ToObjectFunction)
     * @see UnboxTransform#unboxInt(ToObjectFunction)
     * @see UnboxTransform#unboxLong(ToObjectFunction)
     * @see UnboxTransform#unboxShort(ToObjectFunction)
     */
    public static <T> TypedFunction<T> of(ToObjectFunction<T, ?> f) {
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
    public static <T> ToByteFunction<T> unboxBooleanAsByte(ToObjectFunction<T, Boolean> f) {
        return f.mapToByte(BOOLEAN_AS_BYTE);
    }

    /**
     * Equivalent to {@code f.mapLong(DateTimeUtils::epochNanos)}.
     *
     * @param f the instant function
     * @return the epoch nanos function
     * @param <T> the input type
     * @see DateTimeUtils#epochNanos(Instant)
     */
    public static <T> ToLongFunction<T> toEpochNanos(ToObjectFunction<T, Instant> f) {
        return f.mapToLong(EPOCH_NANOS);
    }

    private static class FunctionVisitor<T> implements TypedFunction.Visitor<T, TypedFunction<T>> {

        public static <T> TypedFunction<T> of(TypedFunction<T> f) {
            return f.walk(new FunctionVisitor<>());
        }

        private FunctionVisitor() {}

        @Override
        public TypedFunction<T> visit(ToPrimitiveFunction<T> f) {
            return ToChunkTypeTransform.of(f);
        }

        @Override
        public TypedFunction<T> visit(ToObjectFunction<T, ?> f) {
            return ToChunkTypeTransform.of(f);
        }
    }

    private static class ObjectFunctionVisitor<T> implements
            GenericType.Visitor<TypedFunction<T>>,
            BoxedType.Visitor<TypedFunction<T>> {

        public static <T> TypedFunction<T> of(ToObjectFunction<T, ?> f) {
            return f.returnType().walk(new ObjectFunctionVisitor<>(f));
        }

        private final ToObjectFunction<T, ?> f;

        private ObjectFunctionVisitor(ToObjectFunction<T, ?> f) {
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
